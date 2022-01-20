# rk-db/clickhouse

Init [gorm](https://github.com/go-gorm/gorm) from YAML config.

This belongs to [rk-boot](https://github.com/rookie-ninja/rk-boot) family. We suggest use this lib from [rk-boot](https://github.com/rookie-ninja/rk-boot).

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Supported bootstrap](#supported-bootstrap)
- [Supported Instances](#supported-instances)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [0.Import rk-boot/gin as web framework to use](#0import-rk-bootgin-as-web-framework-to-use)
  - [1.Create boot.yaml](#1create-bootyaml)
  - [2.Create main.go](#2create-maingo)
  - [3.Start server](#3start-server)
  - [4.Validation](#4validation)
    - [4.1 Create user](#41-create-user)
    - [4.1 Update user](#41-update-user)
    - [4.1 List users](#41-list-users)
    - [4.1 Get user](#41-get-user)
    - [4.1 Delete user](#41-delete-user)
- [YAML Options](#yaml-options)
  - [Usage of locale](#usage-of-locale)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Supported bootstrap
| Bootstrap | Description |
| --- | --- |
| YAML based | Start [gorm](https://github.com/go-gorm/gorm) from YAML |
| Code based | Start [gorm](https://github.com/go-gorm/gorm) from code |

## Supported Instances
All instances could be configured via YAML or Code.

**User can enable anyone of those as needed! No mandatory binding!**

| Instance | Description |
| --- | --- |
| gorm.DB | Compatible with original [gorm](https://github.com/go-gorm/gorm) |
| Logger | Implementation of [gorm](https://github.com/go-gorm/gorm) wrapped by [uber-go/zap](https://github.com/uber-go/zap) logger |
| AutoCreation | Automatically create DB if missing in ClickHouse |

## Installation
`go get github.com/rookie-ninja/rk-db/clickhouse`

## Quick Start
In the bellow example, we will run ClickHouse locally and implement API of Create/List/Get/Update/Delete for User model with Gin.

- GET /v1/user, List users
- GET /v1/user/:id, Get user
- PUT /v1/user, Create user
- POST /v1/user/:id, Update user
- DELETE /v1/user/:id, Delete user

Please refer example at [example](example).

### 0.Import rk-boot/gin as web framework to use

```
go get github.com/rookie-ninja/rk-boot/gin
```

### 1.Create boot.yaml
[boot.yaml](example/boot.yaml)

- Create web server with Gin framework at port 8080
- Create ClickHouse entry which connects ClickHouse at localhost:9000

```yaml
---
gin:
  - name: user-service
    port: 8080
    enabled: true
clickHouse:
  - name: user-db                          # Required
    enabled: true                          # Required
    locale: "*::*::*::*"                   # Required
    addr: "localhost:9000"                 # Optional, default: localhost:9000
    user: default                          # Optional, default: default
    pass: ""                               # Optional, default: ""
    database:
      - name: user                         # Required
        autoCreate: true                   # Optional, default: false
#        dryRun: false                     # Optional, default: false
#        params: []                        # Optional, default: []
#    logger:
#      level: warn                         # Optional, default: warn
#      encoding: json                      # Optional, default: console
#      outputPaths: [ "clickhouse/log" ]   # Optional, default: []
```

### 2.Create main.go

In the main() function, we implement two things.

- Add User{} as auto migrate option which will create table in DB automatically if missing.
- Register APIs into Gin router.

```go
// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.
package main

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/rookie-ninja/rk-boot"
	"github.com/rookie-ninja/rk-boot/gin"
	"github.com/rookie-ninja/rk-db/clickhouse"
	"github.com/rs/xid"
	"gorm.io/gorm"
	"net/http"
	"time"
)

var userDb *gorm.DB

func main() {
	boot := rkboot.NewBoot()

	boot.Bootstrap(context.TODO())

	// Auto migrate database and init global userDb variable
	clickHouseEntry := rkclickhouse.GetClickHouseEntry("user-db")
	userDb = clickHouseEntry.GetDB("user")
	if !userDb.DryRun {
		userDb.AutoMigrate(&User{})
	}

	// Register APIs
	ginEntry := rkbootgin.GetGinEntry("user-service")
	ginEntry.Router.GET("/v1/user", ListUsers)
	ginEntry.Router.GET("/v1/user/:id", GetUser)
	ginEntry.Router.PUT("/v1/user", CreateUser)
	ginEntry.Router.POST("/v1/user/:id", UpdateUser)
	ginEntry.Router.DELETE("/v1/user/:id", DeleteUser)

	boot.WaitForShutdownSig(context.TODO())
}

// *************************************
// *************** Model ***************
// *************************************

type Base struct {
	CreatedAt time.Time      `yaml:"-" json:"-"`
	UpdatedAt time.Time      `yaml:"-" json:"-"`
}

type User struct {
	Base
	Id   string `yaml:"id" json:"id"`
	Name string `yaml:"name" json:"name"`
}

func ListUsers(ctx *gin.Context) {
	userList := make([]*User, 0)
	res := userDb.Find(&userList)

	if res.Error != nil {
		ctx.JSON(http.StatusInternalServerError, res.Error)
		return
	}
	ctx.JSON(http.StatusOK, userList)
}

func GetUser(ctx *gin.Context) {
	uid := ctx.Param("id")
	user := &User{}
	res := userDb.Find(user, "id = ?", uid)

	if res.Error != nil {
		ctx.JSON(http.StatusInternalServerError, res.Error)
		return
	}
	ctx.JSON(http.StatusOK, user)
}

func CreateUser(ctx *gin.Context) {
	user := &User{
		Id: xid.New().String(),
		Name: ctx.Query("name"),
	}

	res := userDb.Create(user)

	if res.Error != nil {
		ctx.JSON(http.StatusInternalServerError, res.Error)
		return
	}
	ctx.JSON(http.StatusOK, user)
}

func UpdateUser(ctx *gin.Context) {
	uid := ctx.Param("id")
	user := &User{
		Id: uid,
		Name: ctx.Query("name"),
	}

	res := userDb.Where("id = ?", uid).Updates(user)

	if res.Error != nil {
		ctx.JSON(http.StatusInternalServerError, res.Error)
		return
	}

	ctx.JSON(http.StatusOK, user)
}

func DeleteUser(ctx *gin.Context) {
	uid := ctx.Param("id")

	res := userDb.Delete(&User{}, "id = ?", uid)

	if res.Error != nil {
		ctx.JSON(http.StatusInternalServerError, res.Error)
		return
	}

	ctx.String(http.StatusOK, "success")
}
```

### 3.Start server

```
$ go run main.go

2022-01-07T03:11:18.538+0800    INFO    boot/gin_entry.go:913   Bootstrap ginEntry      {"eventId": "181b17a7-591f-419a-95cc-2cda7efc61f2", "entryName": "user-service"}
------------------------------------------------------------------------
endTime=2022-01-07T03:11:18.53883+08:00
startTime=2022-01-07T03:11:18.538741+08:00
elapsedNano=88391
timezone=CST
ids={"eventId":"181b17a7-591f-419a-95cc-2cda7efc61f2"}
app={"appName":"rk","appVersion":"","entryName":"user-service","entryType":"GinEntry"}
env={"arch":"amd64","az":"*","domain":"*","hostname":"lark.local","localIP":"10.8.0.6","os":"darwin","realm":"*","region":"*"}
payloads={"ginPort":8080}
error={}
counters={}
pairs={}
timing={}
remoteAddr=localhost
operation=Bootstrap
resCode=OK
eventStatus=Ended
EOE
2022-01-07T03:11:18.538+0800    INFO    Bootstrap ClickHouse entry      {"entryName": "user-db", "clickHouseUser": "default", "clickHouseAddr": "localhost:9000"}
2022-01-07T03:11:18.538+0800    INFO    creating database user if not exists
2022-01-07T03:11:18.556+0800    INFO    creating successs or database user exists
2022-01-07T03:11:18.556+0800    INFO    connecting to database user
2022-01-07T03:11:18.567+0800    INFO    connecting to database user success
```

### 4.Validation
#### 4.1 Create user
Create a user with name of rk-dev.

```shell script
$ curl -X PUT "localhost:8080/v1/user?name=rk-dev"
{"id":"c7bjufjd0cvqfaenpqjg","name":"rk-dev"}
```

#### 4.1 Update user
Update user name to rk-dev-updated.

```shell script
$ curl -X POST "localhost:8080/v1/user/c7bjufjd0cvqfaenpqjg?name=rk-dev-updated"
{"id":"c7bjufjd0cvqfaenpqjg","name":"rk-dev-updated"}
```

#### 4.1 List users
List users.

```shell script
$ curl -X GET localhost:8080/v1/user
[{"id":"c7bjufjd0cvqfaenpqjg","name":"rk-dev-updated"}]
```

#### 4.1 Get user
Get user with id=c7bjtobd0cvqfaenpqj0.

```shell script
$ curl -X GET localhost:8080/v1/user/c7bjufjd0cvqfaenpqjg
{"id":"c7bjufjd0cvqfaenpqjg","name":"rk-dev-updated"}
```

#### 4.1 Delete user

```shell script
$ curl -X DELETE localhost:8080/v1/user/c7bjufjd0cvqfaenpqjg
success
```

![image](docs/img/clickhouse.png)

## YAML Options
User can start multiple [gorm](https://github.com/go-gorm/gorm) instances at the same time. Please make sure use different names.

| name | Required | description | type | default value |
| ------ | ------ | ------ | ------ | ------ |
| clickHouse.name | Required | The name of entry | string | ClickHouse |
| clickHouse.enabled | Required | Enable entry or not | bool | false |
| clickHouse.locale | Required | See locale description bellow | string | "" |
| clickHouse.description | Optional | Description of echo entry. | string | "" |
| clickHouse.user | Optional | ClickHouse username | string | root |
| clickHouse.pass | Optional | ClickHouse password | string | pass |
| clickHouse.addr | Optional | ClickHouse remote address | string | localhost:9000 |
| clickHouse.database.name | Required | Name of database | string | "" |
| clickHouse.database.autoCreate | Optional | Create DB if missing | bool | false |
| clickHouse.database.dryRun | Optional | Run gorm.DB with dry run mode | bool | false |
| clickHouse.database.params | Optional | Connection params | []string | [""] |
| clickHouse.logger.encoding | Optional | Log encoding type, json & console are available options | string | console |
| clickHouse.logger.outputPaths | Optional | Output paths of logger | []string | [stdout] |
| clickHouse.logger.level | Optional | Logger level, options: silent, error, warn, info | string | warn |

### Usage of locale

```
RK use <realm>::<region>::<az>::<domain> to distinguish different environment.
Variable of <locale> could be composed as form of <realm>::<region>::<az>::<domain>
- realm: It could be a company, department and so on, like RK-Corp.
         Environment variable: REALM
         Eg: RK-Corp
         Wildcard: supported

- region: Please see AWS web site: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html
          Environment variable: REGION
          Eg: us-east
          Wildcard: supported

- az: Availability zone, please see AWS web site for details: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html
      Environment variable: AZ
      Eg: us-east-1
      Wildcard: supported

- domain: Stands for different environment, like dev, test, prod and so on, users can define it by themselves.
          Environment variable: DOMAIN
          Eg: prod
          Wildcard: supported

How it works?
First, we will split locale with "::" and extract realm, region, az and domain.
Second, get environment variable named as REALM, REGION, AZ and DOMAIN.
Finally, compare every element in locale variable and environment variable.
If variables in locale represented as wildcard(*), we will ignore comparison step.

Example:
# let's assuming we are going to define DB address which is different based on environment.
# Then, user can distinguish DB address based on locale.
# We recommend to include locale with wildcard.
---
DB:
  - name: redis-default
    locale: "*::*::*::*"
    addr: "192.0.0.1:6379"
  - name: redis-in-test
    locale: "*::*::*::test"
    addr: "192.0.0.1:6379"
  - name: redis-in-prod
    locale: "*::*::*::prod"
    addr: "176.0.0.1:6379"
```