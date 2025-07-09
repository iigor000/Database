package main

import (
	"fmt"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/fun"
)

func main() {
	config, err := config.LoadConfigFile("config/config.json")
	if err != nil {
		fmt.Println("Error loading config:", err)
		return
	}

	db, err := fun.NewDatabase(config)
	if err != nil {
		fmt.Println("Error creating database:", err)
		return
	}

	fmt.Println("NoSQL Database")
	var exit bool = false
	for !exit {

		fmt.Println("Choose\n1.PUT\n2.GET\n3.DELETE\n4.EXIT")
		var choice int
		fmt.Scan(&choice)
		switch choice {
		case 1:
			// PUT
			fmt.Println("Enter the key")
			var key string
			fmt.Scan(&key)
			fmt.Println("Enter the value")
			var value string
			fmt.Scan(&value)
			// TODO: Implement PUT
			err := db.Put(key, value)
			if err != nil {
				fmt.Println(err)
				exit = true
				break
			}
			fmt.Println("Data inserted successfully")
		case 2:
			// GET
			fmt.Println("Enter the key")
			var key string
			fmt.Scan(&key)
			// TODO: Implement GET
			value, found := db.Get(key)
			if !found {
				fmt.Println("Entry not found")
			} else {
				fmt.Println(string(value))
			}
		case 3:
			// DELETE
			fmt.Println("Enter the key")
			var key string
			fmt.Scan(&key)
			// TODO: Implement DELETE
			err := db.Delete(key)
			if err != nil {
				fmt.Println(err)
				exit = true
				break
			}
			fmt.Println("Succesfully deleted entry")
		case 4:
			fmt.Println("Goodbye!")
			exit = true
		default:
			fmt.Println("Invalid choice")
		}

	}
}
