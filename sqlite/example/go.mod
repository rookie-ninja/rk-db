module github.com/rookie-ninja/rk-demo

go 1.16

require (
	github.com/gin-gonic/gin v1.8.0
	github.com/rookie-ninja/rk-boot/v2 v2.2.0
	github.com/rookie-ninja/rk-db/sqlite v0.0.0
	github.com/rookie-ninja/rk-gin/v2 v2.2.0
	gorm.io/gorm v1.22.4
)

replace github.com/rookie-ninja/rk-db/sqlite => ../
