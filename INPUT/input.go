package input

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"hackathon-asapcard/INPUT/src/utils"
	"os"

	"github.com/rabbitmq/amqp091-go"
)

//É exigido que o candidato codifique um programa capaz de ler este arquivo e publicar, para
//cada linha do arquivo, uma mensagem em formato JSON de acordo com um critério de
//parse.

//| ID da Transação | Data da Transação | Documento | Nome | Idade | Valor | Num. de Parcelas |

type Fields struct {
	IDtrans      string  `json:"TRANSACTION_ID"`
	DataTrans    string  `json:"TRANSACTION_DATE"`
	Documento    int     `json:"DOCUMENT"`
	Name         string  `json:"NAME"`
	Age          int     `json:"AGE"`
	Value        float64 `json:"VALUE"`
	Installments int     `json:"INSTALLMENT_NUMBER"`
}

func INPUT() {

	ctx := context.Background()

	conn, err := amqp091.Dial("amqp://localhost:5672")
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	q, err := ch.QueueDeclare("producer", false, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	file, err := os.Open("INPUT/input-data.csv")
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
			IDtrans:      record[0],
			DataTrans:    record[1],
			Documento:    utils.ConvertStringInt(record[2]),
			Name:         record[3],
			Age:          utils.ConvertStringInt(record[4]),
			Value:        utils.ConvertStringFlt(record[5]),
			Installments: utils.ConvertStringInt(record[6]),
		}

		json, err := json.Marshal(campos)
		if err != nil {
			panic(err)
		}

		err = ch.PublishWithContext(
			ctx,
			"",     // exchange
			q.Name, // key
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
