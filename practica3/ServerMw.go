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
	//"encoding/gob"
	"fmt"
	"bufio"
	"net"
	"os"
	//"os/exec" --> Esta se utiliza para ejecutar el lanzar.go
	"log"
	"net/rpc"
	"net/http"
	//"io/ioutil"
	//"strings"
	"prac3/com"
	"sync"
)

type PrimesImpl struct {
	toWorkers			 chan Paquete
	mutex                sync.Mutex
}

type Paquete struct {
    Request com.TPInterval
    Resp chan *[]int
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

// PRE: verdad
// POST: IsPrime devuelve verdad si n es primo y falso en caso contrario
func IsPrime(n int) (foundDivisor bool) {
	foundDivisor = false
	for i := 2; (i < n) && !foundDivisor; i++ {
		foundDivisor = (n%i == 0)
	}
	return !foundDivisor
}


// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
// 		intervalo [interval.A, interval.B]
func FindPrimes(interval com.TPInterval) (primes []int) {
	for i := interval.A; i <= interval.B; i++ {
		if IsPrime(i) {
			primes = append(primes, i)
		}
	}
	return primes
}


func enMarcha (IpPort string, toWorkers <-chan Paquete){

	//Partir el string
	//var info = strings.Split(IpPort," ")
	
	//lanzar.go con argumentos puerto e IP. Info[0] es la ip del server para lanzar.go, info[1] el puerto que se pasa al server como parámetro
	//cmd := exec.Command("go","run","lanzar.go", IpPort) 
	//err := cmd.Start()
	//checkError(err)

	//var endpoint = info[0]+":"+info[1]

	for{
		fmt.Println("Esperando intervalo")
		var recibe = <- toWorkers
		fmt.Println("Llega intervalo")


		/*var chanResp = make(chan *[]int)
		var chanTmout = make(chan int)
		var numTouts = 0 // Sumar 1 cada vez que se llegue a un timeout
		go sendRequest(IpPort,recibe,chanResp)
		
		for numTouts != 4{ // Se espera 2 segundos a que se reciba el mensaje
			go func(){ // Funcion que espera 500ms y manda un mensaje por el canal para que se vuelva a esperar
				time.Sleep(time.Duration(500) * time.Millisecond)
				chanTmout <- 1
			}

			select{
				case reply := <- chanResp:

				case tmout := <- chanTmout:
					numTouts++;
					if(numTouts == 4){ // Enviar a otra worker o devolver error??

					}
			}
		}*/

		client, err := rpc.DialHTTP("tcp", IpPort)
		if err != nil { 
			log.Fatal("dialing:", err)
			os.Exit(1)
		}
		fmt.Println("Se hace dial")
		var reply []int
		err = client.Call("PrimesImpl.FindPrimes", recibe.Request, &reply)
		//checkError(err)
		if err != nil { // Se ha producido un crash, enviar por el toWorkers a otro worker a ver si hay más suerte
			log.Fatal("primes error:", err)
			toWorkers <- recibe
			// Intentar volver a poner en marcha el worker?????????
		}

		recibe.Resp <- &reply
	}
}

func  (p *PrimesImpl) FindPrimes(interval com.TPInterval, primeList *[]int)error{

	fmt.Println("Se recibe un cliente")
	var chanResp = make(chan *[]int, 10)
	var recibe Paquete
	recibe.Request = interval
	recibe.Resp = chanResp
	p.toWorkers <- recibe

	primeList = <-chanResp
	return nil
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
