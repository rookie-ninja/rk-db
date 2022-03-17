module github.com/rookie-ninja/rk-demo

go 1.16

require (
	github.com/gin-gonic/gin v1.7.7
	github.com/rookie-ninja/rk-boot/v2 v2.0.1
	github.com/rookie-ninja/rk-db/sqlite v0.0.0
	github.com/rookie-ninja/rk-gin/v2 v2.1.0
	gorm.io/gorm v1.22.4
)

replace github.com/rookie-ninja/rk-db/sqlite => ../
