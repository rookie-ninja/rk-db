module github.com/rookie-ninja/rk-demo

go 1.16

replace github.com/rookie-ninja/rk-db/redis => ../

require (
	github.com/rookie-ninja/rk-boot v1.4.4
	github.com/rookie-ninja/rk-boot/gin v1.2.17
	github.com/rookie-ninja/rk-db/redis v0.0.0-00010101000000-000000000000
)
