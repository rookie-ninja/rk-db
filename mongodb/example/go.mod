module github.com/rookie-ninja/rk-demo

go 1.16

require (
	github.com/gin-gonic/gin v1.7.7
	github.com/rookie-ninja/rk-boot v1.4.4
	github.com/rookie-ninja/rk-boot/gin v1.2.17
	github.com/rookie-ninja/rk-db/mongodb v0.0.0-00010101000000-000000000000
	go.mongodb.org/mongo-driver v1.8.2
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
)

replace github.com/rookie-ninja/rk-db/mongodb => ../
