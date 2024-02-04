package conciliation

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"hackathon-asapcard/INPUT/src/utils"
	"os"

	"github.com/rabbitmq/amqp091-go"
)

// O programa que processa o primeiro arquivo deve ser alterado, passando a inserir
// todas as transações com STATUS pendente (defina a melhor maneira para fazer
// isso).

// O programa que processará o segundo arquivo deverá modificar o valor de cada
// transação registrada na tabela TRANSACTION de acordo com o que chega no
// campo Status do arquivo (defina a melhor maneira para fazer isso).

//| ID da Transação | Data da Transação | Documento | Status |

type Fields struct {
	IDtrans   string `json:"TRANSACTION_ID"`
	DataTrans string `json:"TRANSACTION_DATE"`
	Documento int    `json:"DOCUMENT"`
	Status    string `json:"STATUS"`
}

func CONCILIATION() {

	ctx := context.Background()

	conn, err := amqp091.Dial("amqp://localhost:5672")
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	C, err := ch.QueueDeclare("statusConcluido", false, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	N, err := ch.QueueDeclare("statusNegado", false, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	file, err := os.Open("CONCILIATION/conciliation-data.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break // End of file
			}
		}

		var campos Fields = Fields{
			IDtrans:   record[0],
			DataTrans: record[1],
			Documento: utils.ConvertStringInt(record[2]),
			Status:    record[3],
		}

		if record[3] == "C" {
			json, err := json.Marshal(campos)
			if err != nil {
				panic(err)
			}

			err = ch.PublishWithContext(
				ctx,
				"",     // exchange
				C.Name, // key
				false,  // mandatory
				false,  // immediate
				amqp091.Publishing{
					ContentType: "application/json",
					Body:        json,
				})
			if err != nil {
				panic(err)
			}
		} else {
			if record[3] == "N" {
				json, err := json.Marshal(campos)
				if err != nil {
					panic(err)
				}

				err = ch.PublishWithContext(
					ctx,
					"",     // exchange
					N.Name, // key
					false,  // mandatory
					false,  // immediate
					amqp091.Publishing{
						ContentType: "application/json",
						Body:        json,
					})
				if err != nil {
					panic(err)
				}
			}
		}

	}

}
