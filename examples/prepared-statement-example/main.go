package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	pinot "github.com/startreedata/pinot-client-go/pinot"
)

func main() {
	// Connect to Pinot cluster - adjust broker list as needed
	pinotClient, err := pinot.NewFromBrokerList([]string{"localhost:8000"})
	if err != nil {
		log.Fatalln("Failed to create Pinot client:", err)
	}

	fmt.Println("=== PreparedStatement Example for Pinot Go Client ===\n")

	// Example 1: Basic PreparedStatement usage
	fmt.Println("1. Basic PreparedStatement Usage")
	basicPreparedStatementExample(pinotClient)

	// Example 2: Multiple executions with different parameters  
	fmt.Println("\n2. Multiple Executions with Different Parameters")
	multipleExecutionsExample(pinotClient)

	// Example 3: Complex query with multiple parameter types
	fmt.Println("\n3. Complex Query with Multiple Parameter Types")
	complexQueryExample(pinotClient)

	// Example 4: Using ExecuteWithParams convenience method
	fmt.Println("\n4. Using ExecuteWithParams Convenience Method")
	executeWithParamsExample(pinotClient)

	// Example 5: Error handling and best practices
	fmt.Println("\n5. Error Handling and Best Practices")
	errorHandlingExample(pinotClient)

	fmt.Println("\n=== PreparedStatement Examples Complete ===")
}

func basicPreparedStatementExample(client *pinot.Connection) {
	// Create a prepared statement for a simple query
	stmt, err := client.Prepare("baseballStats", 
		"SELECT playerName, sum(homeRuns) as totalHomeRuns FROM baseballStats WHERE teamID = ? GROUP BY playerID, playerName ORDER BY totalHomeRuns DESC LIMIT ?")
	if err != nil {
		log.Printf("Failed to prepare statement: %v", err)
		return
	}
	defer stmt.Close() // Always close prepared statements

	fmt.Printf("Created prepared statement with %d parameters\n", stmt.GetParameterCount())
	fmt.Printf("Query template: %s\n", stmt.GetQuery())

	// Set parameters using type-specific methods
	err = stmt.SetString(1, "OAK") // teamID
	if err != nil {
		log.Printf("Failed to set parameter 1: %v", err)
		return
	}

	err = stmt.SetInt(2, 10) // LIMIT
	if err != nil {
		log.Printf("Failed to set parameter 2: %v", err)
		return
	}

	// Execute the prepared statement
	response, err := stmt.Execute()
	if err != nil {
		log.Printf("Failed to execute prepared statement: %v", err)
		return
	}

	printQueryResults("Top home run hitters for Oakland Athletics", response)
}

func multipleExecutionsExample(client *pinot.Connection) {
	// Create a prepared statement that we'll execute multiple times with different parameters
	stmt, err := client.Prepare("baseballStats",
		"SELECT teamID, COUNT(*) as playerCount, SUM(homeRuns) as totalHomeRuns FROM baseballStats WHERE yearID = ? GROUP BY teamID ORDER BY totalHomeRuns DESC LIMIT ?")
	if err != nil {
		log.Printf("Failed to prepare statement: %v", err)
		return
	}
	defer stmt.Close()

	// Execute for different years
	years := []int{2000, 2005, 2010}
	for _, year := range years {
		fmt.Printf("\n--- Team statistics for year %d ---\n", year)
		
		// Set parameters for this execution
		stmt.SetInt(1, year)
		stmt.SetInt(2, 5) // Top 5 teams

		response, err := stmt.Execute()
		if err != nil {
			log.Printf("Failed to execute for year %d: %v", year, err)
			continue
		}

		printQueryResults(fmt.Sprintf("Top teams by home runs in %d", year), response)

		// Clear parameters for next iteration (optional, but good practice)
		stmt.ClearParameters()
	}
}

func complexQueryExample(client *pinot.Connection) {
	// Create a complex prepared statement with multiple parameter types
	stmt, err := client.Prepare("baseballStats",
		"SELECT playerName, playerID, teamID, yearID, homeRuns, battingAvg " +
		"FROM baseballStats " +
		"WHERE yearID BETWEEN ? AND ? " +
		"AND homeRuns >= ? " +
		"AND teamID IN (?, ?) " +
		"AND battingAvg > ? " +
		"ORDER BY homeRuns DESC " +
		"LIMIT ?")
	if err != nil {
		log.Printf("Failed to prepare complex statement: %v", err)
		return
	}
	defer stmt.Close()

	// Set parameters with different types
	stmt.SetInt(1, 2005)      // Start year
	stmt.SetInt(2, 2010)      // End year  
	stmt.SetInt(3, 20)        // Minimum home runs
	stmt.SetString(4, "NYA")  // Team 1 (New York Yankees)
	stmt.SetString(5, "BOS")  // Team 2 (Boston Red Sox)
	stmt.SetFloat64(6, 0.280) // Minimum batting average
	stmt.SetInt(7, 15)        // Limit

	response, err := stmt.Execute()
	if err != nil {
		log.Printf("Failed to execute complex query: %v", err)
		return
	}

	printQueryResults("Power hitters from Yankees and Red Sox (2005-2010)", response)
}

func executeWithParamsExample(client *pinot.Connection) {
	// Create a prepared statement for player statistics
	stmt, err := client.Prepare("baseballStats",
		"SELECT playerName, AVG(battingAvg) as avgBattingAvg, SUM(homeRuns) as totalHomeRuns, COUNT(*) as seasons " +
		"FROM baseballStats " +
		"WHERE playerName LIKE ? " +
		"GROUP BY playerID, playerName " +
		"HAVING COUNT(*) >= ? " +
		"ORDER BY avgBattingAvg DESC " +
		"LIMIT ?")
	if err != nil {
		log.Printf("Failed to prepare statement: %v", err)
		return
	}
	defer stmt.Close()

	// Use ExecuteWithParams for one-shot execution
	response, err := stmt.ExecuteWithParams("%Smith%", 3, 10)
	if err != nil {
		log.Printf("Failed to execute with params: %v", err)
		return
	}

	printQueryResults("Players with surname 'Smith' (3+ seasons)", response)
}

func errorHandlingExample(client *pinot.Connection) {
	fmt.Println("Demonstrating error handling scenarios:")

	// Example 1: Invalid parameter index
	stmt, err := client.Prepare("baseballStats", "SELECT * FROM baseballStats WHERE playerID = ?")
	if err != nil {
		log.Printf("Failed to prepare statement: %v", err)
		return
	}
	defer stmt.Close()

	// Try to set parameter with invalid index
	err = stmt.SetString(0, "test") // Index should be 1-based
	if err != nil {
		fmt.Printf("✓ Correctly caught invalid parameter index: %v\n", err)
	}

	err = stmt.SetString(2, "test") // Only 1 parameter available
	if err != nil {
		fmt.Printf("✓ Correctly caught out-of-range parameter index: %v\n", err)
	}

	// Example 2: Execute without setting all parameters
	_, err = stmt.Execute()
	if err != nil {
		fmt.Printf("✓ Correctly caught unset parameter: %v\n", err)
	}

	// Example 3: Wrong number of parameters in ExecuteWithParams
	_, err = stmt.ExecuteWithParams("param1", "param2") // Too many params
	if err != nil {
		fmt.Printf("✓ Correctly caught parameter count mismatch: %v\n", err)
	}

	// Example 4: Using closed statement
	stmt.Close()
	err = stmt.SetString(1, "test")
	if err != nil {
		fmt.Printf("✓ Correctly caught usage of closed statement: %v\n", err)
	}
}

func printQueryResults(title string, response *pinot.BrokerResponse) {
	fmt.Printf("--- %s ---\n", title)
	
	if response.Exceptions != nil && len(response.Exceptions) > 0 {
		fmt.Printf("Query returned exceptions: %v\n", response.Exceptions)
		return
	}

	if response.ResultTable == nil {
		fmt.Println("No results returned")
		return
	}

	// Print column headers
	for i := 0; i < response.ResultTable.GetColumnCount(); i++ {
		fmt.Printf("%-20s", response.ResultTable.GetColumnName(i))
	}
	fmt.Println()

	// Print separator
	for i := 0; i < response.ResultTable.GetColumnCount(); i++ {
		fmt.Printf("%-20s", "--------------------")
	}
	fmt.Println()

	// Print data rows (limit to first 10 for readability)
	maxRows := response.ResultTable.GetRowCount()
	if maxRows > 10 {
		maxRows = 10
	}

	for r := 0; r < maxRows; r++ {
		for c := 0; c < response.ResultTable.GetColumnCount(); c++ {
			value := response.ResultTable.Get(r, c)
			fmt.Printf("%-20v", value)
		}
		fmt.Println()
	}

	if response.ResultTable.GetRowCount() > 10 {
		fmt.Printf("... and %d more rows\n", response.ResultTable.GetRowCount()-10)
	}

	fmt.Printf("\nQuery execution time: %d ms\n", response.TimeUsedMs)
}

// Alternative example showing prepared statement reuse pattern
func demonstratePreparedStatementReuse() {
	fmt.Println("\n=== Prepared Statement Reuse Pattern ===")
	
	client, err := pinot.NewFromBrokerList([]string{"localhost:8000"})
	if err != nil {
		log.Printf("Failed to create client: %v", err)
		return
	}

	// Create a prepared statement once
	stmt, err := client.Prepare("baseballStats", 
		"SELECT COUNT(*) as count FROM baseballStats WHERE teamID = ? AND yearID = ?")
	if err != nil {
		log.Printf("Failed to prepare statement: %v", err)
		return
	}
	defer stmt.Close()

	// Reuse the same prepared statement for multiple queries
	teams := []string{"NYA", "BOS", "OAK", "LAA"}
	years := []int{2008, 2009, 2010}

	fmt.Printf("Player counts by team and year:\n")
	fmt.Printf("%-8s", "Team")
	for _, year := range years {
		fmt.Printf("%-8d", year)
	}
	fmt.Println()

	for _, team := range teams {
		fmt.Printf("%-8s", team)
		for _, year := range years {
			// Reuse the prepared statement with new parameters
			response, err := stmt.ExecuteWithParams(team, year)
			if err != nil {
				fmt.Printf("%-8s", "ERROR")
				continue
			}

			if response.ResultTable != nil && response.ResultTable.GetRowCount() > 0 {
				count := response.ResultTable.Get(0, 0)
				fmt.Printf("%-8v", count)
			} else {
				fmt.Printf("%-8s", "0")
			}
		}
		fmt.Println()
	}
} 