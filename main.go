package main

import "fmt"

func main() {
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
		case 2:
			// GET
			fmt.Println("Enter the key")
			var key string
			fmt.Scan(&key)
			// TODO: Implement GET
		case 3:
			// DELETE
			fmt.Println("Enter the key")
			var key string
			fmt.Scan(&key)
			// TODO: Implement DELETE
		case 4:
			fmt.Println("Goodbye!")
			exit = true
		default:
			fmt.Println("Invalid choice")
		}

	}
}
