module github.com/rookie-ninja/rk-demo

go 1.16

require (
	github.com/Shopify/sarama v1.30.0 // indirect
	github.com/gin-gonic/gin v1.8.0
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/rookie-ninja/rk-boot/v2 v2.2.5
	github.com/rookie-ninja/rk-db/mysql v0.0.1
	github.com/rookie-ninja/rk-entry/v2 v2.2.19
	github.com/rookie-ninja/rk-gin/v2 v2.2.7
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.10.0 // indirect
	gorm.io/gorm v1.24.0
)

replace github.com/rookie-ninja/rk-db/mysql => ../
