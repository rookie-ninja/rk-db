module github.com/rookie-ninja/rk-demo

go 1.16

require (
	github.com/gin-gonic/gin v1.8.0
	github.com/rookie-ninja/rk-boot/v2 v2.2.3
	github.com/rookie-ninja/rk-db/mysql v0.0.1
	github.com/rookie-ninja/rk-entry/v2 v2.2.5
	github.com/rookie-ninja/rk-gin/v2 v2.2.5
	gorm.io/gorm v1.23.8
)

replace github.com/rookie-ninja/rk-db/mysql => ../
