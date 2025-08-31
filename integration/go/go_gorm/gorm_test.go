package main

import (
	"context"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/stretchr/testify/assert"
)

type Sharded struct {
	gorm.Model
	Id    int64
	Value string
}

func (Sharded) TableName() string {
	return "sharded"
}

func TestInit(t *testing.T) {
	db, err := gorm.Open(postgres.Open("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog"), &gorm.Config{})

	assert.NoError(t, err)

	ctx := context.Background()

	db.AutoMigrate(&Sharded{})

	db.Exec("TRUNCATE TABLE sharded CASCADE")

	err = gorm.G[Sharded](db).Create(ctx, &Sharded{Id: 1, Value: "test"})
	assert.NoError(t, err)

	for range 25 {
		entry, err := gorm.G[Sharded](db).Where("id = ?", 1).First(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "test", entry.Value)
	}
}
