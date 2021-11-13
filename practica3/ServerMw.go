/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: server.go
* DESCRIPCIÓN: contiene la funcionalidad esencial para realizar los servidores
*				correspondientes al trabajo 1
*/
package main

import (
	"fmt"
	"bufio"
	"net"
	"os"
	"os/exec" 
	"log"
	"net/rpc"
	"net/http"
	"prac3/com"
	"sync"
	"time"
)

type PrimesImpl struct {
	toWorkers			 chan Paquete
	mutex                sync.Mutex
}

type Paquete struct {
    Request 		com.TPInterval
    Resp 			chan *[]int
    tiempoIni		time.Time

}


const ( // Añadido
	CONN_HOST = "localhost"
	CONN_PORT = "20050"
	CONN_TYPE = "tcp"
)

func checkError(err error) {
	if err != nil {
		fmt.Println("Game over amego")
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
// 		intervalo [interval.A, interval.B]

func sendRequest(endpoint string, recibe Paquete, chanResp chan<- *[]int,toWorkers chan Paquete){
	client, err := rpc.DialHTTP("tcp", endpoint)
	if err != nil { 
		log.Fatal("dialing:", err)
		os.Exit(1)
	}
	
	var reply []int
	err = client.Call("PrimesImpl.FindPrimes", recibe.Request, &reply)
	//checkError(err)
	if err != nil { // Se ha producido un crash, enviar por el toWorkers a otro worker a ver si hay más suerte
		fmt.Println("Se ha producido crash ", endpoint)
		//log.Fatal("primes error:", err)
		cmd := exec.Command("go","run","lanzar.go", endpoint) 
		err := cmd.Start()		
		checkError(err)
		reply = []int{-1}
		fmt.Println("Server puesto en marcha de nuevo")
	}
	
	chanResp <- &reply
}



func enMarcha (IpPort string, toWorkers chan Paquete){

	cmd := exec.Command("go","run","lanzar.go", IpPort) 
	err := cmd.Start()
	checkError(err)
	fmt.Println("Server lanzado ",err)

	for{

		var recibe = <- toWorkers
		fmt.Println("Recibido en ", IpPort)
		var chanResp = make(chan *[]int)
		var chanTmout = make(chan int)
		var numTouts = 0 										// Sumar 1 cada vez que se llegue a un timeout
		go sendRequest(IpPort,recibe,chanResp,toWorkers)
		
		for numTouts != 2{ 										// Se espera 2 segundos a que se reciba el mensaje
			go func(){ 											// Funcion que espera 800ms y manda un mensaje por el canal para que se vuelva a esperar
				time.Sleep(time.Duration(700) * time.Millisecond)
				chanTmout <- 1
			}()

			select{ 
				case reply := <- chanResp: 						// Llega respuesta o crash
					numTouts=2
					var first = (*reply)[0]
					//if(first != -1 || time.Since(recibe.tiempoIni) > 2) { // Mirar si ha habido algun error
					/*if(first == -1){
						cmd := exec.Command("go","run","lanzar.go", IpPort) 
						err := cmd.Start()
						checkError(err)
					}*/

					if(first != -1){
						recibe.Resp <- reply 
						fmt.Println("El primero es ",first," en ", IpPort)
					} else if((time.Since(recibe.tiempoIni)).Seconds() > 2){
						recibe.Resp <- reply 
						fmt.Println("El primero es ",first," en ", IpPort, " y ya no hay mas tiempo")
					}else{ 									// Tiempo no expirado, se puede volver a intentar
						fmt.Println("Hay esperanza todavia")
						time.Sleep(time.Duration(1) * time.Second)
						go sendRequest(IpPort,recibe,chanResp,toWorkers)
						numTouts=0
					}
									
				case _ = <- chanTmout: 							// Llega tiemout
					numTouts++;
					fmt.Print("to")
					if(numTouts == 2){ 							// Llega al número de timeouts máximo
						if((time.Since(recibe.tiempoIni)).Seconds() < 2){ 	// Hay tiempo de reenviar a otro worker
							toWorkers <- recibe
							fmt.Println("\nReenvia a otro worker desde ",IpPort)
						}else{// No da tiempo a volver a reenviarlo
							fmt.Println("No hay reenvio desde", IpPort)
							var reply = []int{-1}

							recibe.Resp <- &reply
						}


					}
			}
		}
	}
}

// En FindPrimes empezar un timer y guardarlo en el Paquete. Cada vez que hay un fallo CLASH u OMISSION
// mirar el tiempo que ha pasado y si es > que 2.3 segundos no da tiempo a realizar la tarea por lo que 
// se devuelve el error

func  (p *PrimesImpl) FindPrimes(interval com.TPInterval, primeList *[]int)error{

	fmt.Println("Se recibe un cliente")
	var chanResp = make(chan *[]int, 10)
	var recibe Paquete
	recibe.tiempoIni = time.Now()
	recibe.Request = interval
	recibe.Resp = chanResp
	p.toWorkers <- recibe

	primeList = <-chanResp
	var first = (*primeList)[0]
	fmt.Println("Al cliente se le devuelve ",first, " en ", (time.Since(recibe.tiempoIni)).Seconds())
	if(first == 0){ // Ha habido un error
		return nil
	}else{
		return nil
	}
	
}

func main() {

	if len(os.Args) != 2{
		fmt.Println("Usage: " + os.Args[0] + " <file>")
		os.Exit(1)
	}

	file,err := os.Open(os.Args[1])

	if err != nil{
		fmt.Println("Error al abrir el fichero")
		os.Exit(1)
	}


	var toWorkers = make(chan Paquete, 10)

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
        fmt.Println(scanner.Text())
        go enMarcha(scanner.Text(),toWorkers)
        //Poner en marcha cada uno de los servidores
    }
    file.Close()

    primesImpl := new(PrimesImpl)
    primesImpl.toWorkers = toWorkers
    rpc.Register(primesImpl)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", CONN_HOST+":"+CONN_PORT)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Println("Escuchando a clientes")
    	
    http.Serve(l, nil)
}
