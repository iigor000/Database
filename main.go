package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/iigor000/database/config"
	"github.com/iigor000/database/fun"
)

func main() {
	config, err := config.LoadConfigFile("config/config.json")
	if err != nil {
		fmt.Println("Error loading config:", err)
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	var username string

	fmt.Println("NoSQL Database")
	for {
		fmt.Println("Enter username: ")
		if scanner.Scan() {
			username = strings.TrimSpace(scanner.Text())
			if username != "" {
				break
			}
		}
		fmt.Println("Username cannot be empty. Please try again.")
	}

	db, err := fun.NewDatabase(config, username)
	if err != nil {
		fmt.Println("Error creating database:", err)
		return
	}

	fun.CreateBucket(db)

	helpstr := "help - Print Commands\nput - Add Entry to Database\nget - Get Entry from Database\ndelete - Delete Entry from Database\naddbl - Add BloomFilter\ndelbl - Delete BloomFilter\naddtobl - Add key to BloomFilter\ngetbl - Check key in BloomFilter\naddcms - Add CountMinSketch\ndelcms - Delete CountMinSketch\naddtocms - Add key to CountMinSketch\ngetcms - Check key in CountMinSketch\naddhll - Add HyperLogLog\ndelhll - Delete HyperLogLog\naddtohll - Add key to HyperLogLog\ngethll - Estimate HyperLogLog\naddfp - Add Fingerprint of text\ndelfp - Delete fingerprint\nvalidate - Validate Merkle Tree \nexit - Exit"
	fmt.Println(helpstr)

	var exit bool = false
	for !exit && scanner.Scan() {
		choice := strings.ToLower(strings.TrimSpace(scanner.Text()))
		println("")

		switch choice {
		case "help":
			fmt.Println(helpstr)
		case "put":
			fmt.Println("Enter the key")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())

			fmt.Println("Enter the value")
			if !scanner.Scan() {
				break
			}
			value := strings.TrimSpace(scanner.Text())

			err := db.Put(key, []byte(value))
			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Println("Data inserted successfully")
		case "get":
			fmt.Println("Enter the key")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())

			value, found, err := db.Get(key)
			if err != nil {
				fmt.Println("Error retrieving data:", err)
				break
			}
			if !found {
				fmt.Println("Entry not found")
			} else {
				fmt.Println(string(value))
			}
		case "delete":
			fmt.Println("Enter the key")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())

			err := db.Delete(key)
			if err != nil {
				fmt.Println(err)
				exit = true
				break
			}
			fmt.Println("Successfully deleted entry")
		case "addbl":
			fmt.Println("Enter the key for BloomFilter")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())

			fmt.Println("Enter the expected number of elements")
			if !scanner.Scan() {
				break
			}
			expectedElements, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
			if err != nil {
				fmt.Println("Invalid number:", err)
				break
			}

			fmt.Println("Enter the false positive probability (between 0 and 1)")
			if !scanner.Scan() {
				break
			}
			falsePositiveProbability, err := strconv.ParseFloat(strings.TrimSpace(scanner.Text()), 64)
			if err != nil {
				fmt.Println("Invalid number:", err)
				break
			}

			err = db.NewBloomFilter(key, expectedElements, falsePositiveProbability)
			if err != nil {
				fmt.Println("Error creating BloomFilter:", err)
			}
		case "delbl":
			fmt.Println("Enter the key for BloomFilter")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())

			err := db.DeleteBloomFilter(key)
			if err != nil {
				fmt.Println("Error deleting BloomFilter:", err)
			} else {
				fmt.Println("BloomFilter deleted successfully")
			}
		case "addtobl":
			fmt.Println("Enter the key for BloomFilter")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())

			fmt.Println("Enter the value to add to BloomFilter")
			if !scanner.Scan() {
				break
			}
			value := strings.TrimSpace(scanner.Text())

			err := db.AddToBloomFilter(key, []byte(value))
			if err != nil {
				fmt.Println("Error adding to BloomFilter:", err)
				break
			}
			fmt.Println("Value added to BloomFilter successfully")
		case "getbl":
			fmt.Println("Enter the key for BloomFilter")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())

			fmt.Println("Enter the value to check in BloomFilter")
			if !scanner.Scan() {
				break
			}
			value := strings.TrimSpace(scanner.Text())

			found, err := db.CheckInBloomFilter(key, []byte(value))
			if err != nil {
				fmt.Println("Error checking BloomFilter:", err)
			} else if found {
				fmt.Println("Value is likely in the BloomFilter")
			} else {
				fmt.Println("Value is definitely not in the BloomFilter")
			}
		case "addcms":
			fmt.Println("Enter the key for CountMinSketch")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())
			fmt.Println("Enter error rate, between 0 and 1")
			if !scanner.Scan() {
				break
			}
			errorRate, err := strconv.ParseFloat(strings.TrimSpace(scanner.Text()), 64)
			if err != nil {
				fmt.Println("Invalid error rate:", err)
				break
			}
			fmt.Println("Enter the confidence level, between 0 and 1")
			if !scanner.Scan() {
				break
			}
			confidenceLevel, err := strconv.ParseFloat(strings.TrimSpace(scanner.Text()), 64)
			if err != nil {
				fmt.Println("Invalid confidence level:", err)
				break
			}
			err = db.CreateCMS(key, errorRate, confidenceLevel)
			if err != nil {
				fmt.Println("Error creating CountMinSketch:", err)
			} else {
				fmt.Println("CountMinSketch created successfully")
			}
		case "delcms":
			fmt.Println("Enter the key for CountMinSketch")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())
			err := db.DeleteCMS(key)
			if err != nil {
				fmt.Println("Error deleting CountMinSketch:", err)
			} else {
				fmt.Println("CountMinSketch deleted successfully")
			}
		case "addtocms":
			fmt.Println("Enter the key for CountMinSketch")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())
			fmt.Println("Enter the value to add to CountMinSketch")
			if !scanner.Scan() {
				break
			}
			value := strings.TrimSpace(scanner.Text())
			err := db.AddToCMS(key, []byte(value))
			if err != nil {
				fmt.Println("Error adding to CountMinSketch:", err)
			} else {
				fmt.Println("Value added to CountMinSketch successfully")
			}
		case "getcms":
			fmt.Println("Enter the key for CountMinSketch")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())
			fmt.Println("Enter the value to check in CountMinSketch")
			if !scanner.Scan() {
				break
			}
			value := strings.TrimSpace(scanner.Text())
			count, err := db.CheckInCMS(key, []byte(value))
			if err != nil {
				fmt.Println("Error checking CountMinSketch:", err)
			} else {
				fmt.Printf("Count for '%s' in CountMinSketch '%s': %d\n", value, key, count)
			}
		case "addhll":
			fmt.Println("Enter the key for HyperLogLog")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())
			fmt.Println("Enter the precision (between 4 and 16)")
			if !scanner.Scan() {
				break
			}
			precisionStr := strings.TrimSpace(scanner.Text())
			precision, err := strconv.Atoi(precisionStr)
			if err != nil {
				fmt.Println("Invalid precision:", err)
				break
			}
			err = db.CreateHLL(key, precision)
			if err != nil {
				fmt.Println("Error creating HyperLogLog:", err)
			} else {
				fmt.Println("HyperLogLog created successfully")
			}
		case "delhll":
			fmt.Println("Enter the key for HyperLogLog")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())
			err := db.DeleteHLL(key)
			if err != nil {
				fmt.Println("Error deleting HyperLogLog:", err)
			} else {
				fmt.Println("HyperLogLog deleted successfully")
			}
		case "addtohll":
			fmt.Println("Enter the key for HyperLogLog")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())
			fmt.Println("Enter the value to add to HyperLogLog")
			if !scanner.Scan() {
				break
			}
			value := strings.TrimSpace(scanner.Text())
			err := db.AddToHLL(key, []byte(value))
			if err != nil {
				fmt.Println("Error adding to HyperLogLog:", err)
			} else {
				fmt.Println("Value added to HyperLogLog successfully")
			}
		case "gethll":
			fmt.Println("Enter the key for HyperLogLog")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())
			count, err := db.EstimateHLL(key)
			if err != nil {
				fmt.Println("Error estimating HyperLogLog:", err)
			} else {
				fmt.Printf("Estimated count for HyperLogLog '%s': %f\n", key, count)
			}
		case "addfp":
			fmt.Println("Enter the key for the fingerprint")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())

			fmt.Println("Enter the text to fingerprint")
			if !scanner.Scan() {
				break
			}
			text := strings.TrimSpace(scanner.Text())

			err := db.AddSHFingerprint(key, text)
			if err != nil {
				fmt.Println("Error adding fingerprint:", err)
			} else {
				fmt.Println("Fingerprint added successfully")
			}
		case "delfp":
			fmt.Println("Enter the key for the fingerprint")
			if !scanner.Scan() {
				break
			}
			key := strings.TrimSpace(scanner.Text())

			err := db.DeleteSHFingerprint(key)
			if err != nil {
				fmt.Println("Error deleting fingerprint:", err)
			} else {
				fmt.Println("Fingerprint deleted successfully")
			}
		case "getdist":
			fmt.Println("Enter the first key for the fingerprint")
			if !scanner.Scan() {
				break
			}
			key1 := strings.TrimSpace(scanner.Text())

			fmt.Println("Enter the second key for the fingerprint")
			if !scanner.Scan() {
				break
			}
			key2 := strings.TrimSpace(scanner.Text())

			distance, err := db.GetHemmingDistance(key1, key2)
			if err != nil {
				fmt.Println("Error getting Hemingway distance:", err)
			} else {
				fmt.Printf("Hemingway distance between '%s' and '%s': %d\n", key1, key2, distance)
			}
		case "validate":
			fmt.Println("Input the generation")
			if !scanner.Scan() {
				break
			}
			generation := strings.TrimSpace(scanner.Text())
			fmt.Println("Input the level")
			if !scanner.Scan() {
				break
			}
			level := strings.TrimSpace(scanner.Text())
			generationInt, err := strconv.Atoi(generation)
			if err != nil {
				fmt.Println("Invalid generation:", err)
				break
			}
			levelInt, err := strconv.Atoi(level)
			if err != nil {
				fmt.Println("Invalid level:", err)
				break
			}
			err = db.ValidateMerkleTree(generationInt, levelInt)
			if err != nil {
				fmt.Println("Error validating Merkle Tree:", err)
			} else {
				fmt.Println("Merkle Tree validated successfully")
			}
		case "exit":
			fmt.Println("Goodbye!")
			db.Close()
			exit = true
		default:
			if choice != "" {
				fmt.Println("Invalid choice")
			}
		}
	}
}
