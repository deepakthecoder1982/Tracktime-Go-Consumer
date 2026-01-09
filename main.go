package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"os"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/joho/godotenv"
)

var (
	batchSize     = 1
	batchMessages []string
)
var db *sql.DB

type InfoData struct {
    ActivityUUID       string    `json:"activity_uuid"`
    UserUID            string    `json:"user_id"`
    OrganizationID     string    `json:"organization_id"`
    Timestamp          time.Time `json:"timestamp"`
    AppName            string    `json:"app_name"`
    URL                string    `json:"url"`
    PageTitle          string    `json:"page_title"`
    ProductivityStatus string    `json:"productivity_status"`
    Meridian           string    `json:"meridian"`
    IPAddress          string    `json:"ip_address"`
    MacAddress         string    `json:"mac_address"`
    MouseMovement      bool      `json:"mouse_movement"`
    MouseClicks        int       `json:"mouse_clicks"`
    KeysClicks         int       `json:"keys_clicks"`
    Status             int       `json:"status"`
    CPUUsage           string    `json:"cpu_usage"`
    RAMUsage           string    `json:"ram_usage"`
    ScreenshotUID      string    `json:"screenshot_uid"`
    ThumbnailUID       string    `json:"thumbnail_uid"`
    Device_user_name   string    `json:"device_user_name"`
}

func main() {
	err := godotenv.Load()
    if err != nil {
        log.Fatalf("Error loading .env file: %v", err)
    }
	
	connStr := os.Getenv("POSTGRES_CONN_STR")
	db, err := sql.Open("postgres", connStr)
    if err != nil {
        panic(err)
    }
    defer db.Close()

    if err := db.Ping(); err != nil {
        fmt.Println("Error connecting to the database:", err)
        return
    }

    fmt.Println("Connected to the PostgreSQL database")

    if err := ensureTableExists(db); err != nil {
        log.Fatalf("Error ensuring table exists: %v", err)
    }

    inspectTableStructure(db)

	// Kafka settings with proper consumer group
    userName := os.Getenv("KAFKA_USER_NAME")
    password := os.Getenv("KAFKA_PASSWORD")
	mechanism, err := scram.Mechanism(scram.SHA256, userName, password)
	if err != nil {
		log.Fatalln(err)
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

    topic := os.Getenv("TOPIC")

	// ✅ FIXED: Added GroupID for proper offset management
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{os.Getenv("KAFKA_BROKER")},
		Topic:       topic,
		GroupID:     "productivity-tracker-consumer", // ✅ Critical fix
		Dialer:      dialer,
		StartOffset: kafka.LastOffset, // Start from latest for new consumers
	})
	defer r.Close()

	fmt.Println("Kafka consumer started with group ID: productivity-tracker-consumer")

	// Kafka consumer loop
	for {
        ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
        
        m, err := r.ReadMessage(ctx)
        if err != nil {
            if err == context.DeadlineExceeded {
                fmt.Println("No new messages, waiting...")
            } else {
                fmt.Println("Error reading Kafka message:", err)
            }
            cancel()
            continue
        }

        fmt.Printf("Received message at offset %d: %s\n", m.Offset, string(m.Value))
        batchMessages = append(batchMessages, string(m.Value))

        if len(batchMessages) >= batchSize {
            processBatch(db, batchMessages)
            batchMessages = nil
        }

        cancel()
    }
}

func ensureTableExists(db *sql.DB) error {
    exists, err := tableExists(db, "user_activity")
    if err != nil {
        return err
    }
    if !exists {
        return createNewTable(db)
    } else {
        return validateTableSchema(db)
    }
}

func tableExists(db *sql.DB, tableName string) (bool, error) {
    var count int
    err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1", tableName).Scan(&count)
    if err != nil {
        return false, err
    }
    return count > 0, nil
}

func createNewTable(db *sql.DB) error {
    createTableSQL := `
    CREATE TABLE IF NOT EXISTS user_activity (
        activity_uuid VARCHAR(255) PRIMARY KEY,
        user_uid VARCHAR(255),
        organization_id VARCHAR(255),
        timestamp TIMESTAMP,
        app_name VARCHAR(255),
        url VARCHAR(255),
        page_title VARCHAR(255),
        productivity_status VARCHAR(255),
        meridian VARCHAR(255),
        ip_address VARCHAR(255),
        mac_address VARCHAR(255),
        mouse_movement BOOLEAN,
        mouse_clicks INTEGER,
        keys_clicks INTEGER,
        status INTEGER,
        cpu_usage VARCHAR(255),
        ram_usage VARCHAR(255),
        screenshot_uid VARCHAR(255),
        thumbnail_uid VARCHAR(255), 
        device_user_name VARCHAR(50)
    );`

    _, err := db.Exec(createTableSQL)
    if err != nil {
        return err
    }

    fmt.Println("Table 'user_activity' created successfully.")
    return nil
}

func validateTableSchema(db *sql.DB) error {
    query := `
    SELECT column_name, data_type, is_nullable 
    FROM information_schema.columns 
    WHERE table_name = 'user_activity' AND table_schema = 'public'
    ORDER BY ordinal_position;
    `
    
    rows, err := db.Query(query)
    if err != nil {
        return err
    }
    defer rows.Close()

    // ✅ FIXED: Added thumbnail_uid to expected columns
    expectedColumns := map[string]bool{
        "activity_uuid": false,
        "user_uid": false,
        "organization_id": false,
        "timestamp": false,
        "app_name": false,
        "url": false,
        "page_title": false,
        "productivity_status": false,
        "meridian": false,
        "ip_address": false,
        "mac_address": false,
        "mouse_movement": false,
        "mouse_clicks": false,
        "keys_clicks": false,
        "status": false,
        "cpu_usage": false,
        "ram_usage": false,
        "screenshot_uid": false,
        "thumbnail_uid": false,    // ✅ Added this
        "device_user_name": false,
    }

    schemaIssues := false
    for rows.Next() {
        var columnName, dataType, isNullable string
        if err := rows.Scan(&columnName, &dataType, &isNullable); err != nil {
            return err
        }
        
        if _, exists := expectedColumns[columnName]; exists {
            expectedColumns[columnName] = true
        } else {
            fmt.Printf("WARNING: Unexpected column found: %s\n", columnName)
            schemaIssues = true
        }
    }

    // Check for missing columns
    for col, found := range expectedColumns {
        if !found {
            fmt.Printf("ERROR: Missing column: %s\n", col)
            schemaIssues = true
        }
    }

    if schemaIssues {
        fmt.Println("Schema issues detected. Recreating table...")
        return recreateTable(db)
    }

    fmt.Println("Table schema validation passed.")
    return nil
}

func recreateTable(db *sql.DB) error {
    dropSQL := "DROP TABLE IF EXISTS user_activity;"
    _, err := db.Exec(dropSQL)
    if err != nil {
        return fmt.Errorf("error dropping table: %v", err)
    }
    
    fmt.Println("Dropped existing table.")
    return createNewTable(db)
}

func processBatch(db *sql.DB, messages []string) {
    for _, message := range messages {
        var infoData InfoData
        if err := json.Unmarshal([]byte(message), &infoData); err != nil {
            log.Printf("Error unmarshalling message: %v\n", err)
            continue
        }

        if err := insertOrUpdateProject(db, infoData); err != nil {
            log.Printf("Error inserting/updating data: %v\n", err)
        }
		confirmDataAdded(db)
    }
}

// ✅ FIXED: Added duplicate prevention
func insertOrUpdateProject(db *sql.DB, data InfoData) error {
    // Check if record already exists
    var count int
    checkSQL := "SELECT COUNT(*) FROM user_activity WHERE activity_uuid = $1"
    err := db.QueryRow(checkSQL, data.ActivityUUID).Scan(&count)
    if err != nil {
        return err
    }
    
    if count > 0 {
        fmt.Printf("Record with activity_uuid %s already exists, skipping...\n", data.ActivityUUID)
        return nil // Skip duplicate
    }

    // Insert new record
    sqlStatement := `
    INSERT INTO user_activity (activity_uuid, user_uid, organization_id, timestamp, app_name, url, page_title, productivity_status, meridian, ip_address, mac_address, mouse_movement, mouse_clicks, keys_clicks, status, cpu_usage, ram_usage, screenshot_uid, thumbnail_uid, device_user_name)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
    `
    
    fmt.Printf("Inserting new record for user-id: %s\n", data.UserUID)

    _, err = db.Exec(sqlStatement, data.ActivityUUID, data.UserUID, data.OrganizationID, data.Timestamp, data.AppName, data.URL, data.PageTitle, data.ProductivityStatus, data.Meridian, data.IPAddress, data.MacAddress, data.MouseMovement, data.MouseClicks, data.KeysClicks, data.Status, data.CPUUsage, data.RAMUsage, data.ScreenshotUID, data.ThumbnailUID, data.Device_user_name)
    if err != nil {
        return err
    }

    fmt.Println("Data inserted successfully.")
    return nil
}

func confirmDataAdded(db *sql.DB) {
    query := "SELECT COUNT(*) FROM user_activity"
    var count int
    err := db.QueryRow(query).Scan(&count)
    if err != nil {
        log.Fatalf("Error querying the database: %v", err)
    }

    fmt.Printf("Total records in user_activity table: %d\n", count)
}

func inspectTableStructure(db *sql.DB) {
    query := `
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns 
    WHERE table_name = 'user_activity' AND table_schema = 'public'
    ORDER BY ordinal_position;
    `
    
    rows, err := db.Query(query)
    if err != nil {
        log.Printf("Error inspecting table structure: %v", err)
        return
    }
    defer rows.Close()

    fmt.Println("Current table structure:")
    fmt.Println("Column Name | Data Type | Nullable | Default")
    fmt.Println("-----------|-----------|----------|--------")
    
    for rows.Next() {
        var columnName, dataType, isNullable string
        var columnDefault sql.NullString
        
        if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault); err != nil {
            log.Printf("Error scanning row: %v", err)
            continue
        }
        
        defaultVal := "NULL"
        if columnDefault.Valid {
            defaultVal = columnDefault.String
        }
        
        fmt.Printf("%-15s | %-12s | %-8s | %s\n", columnName, dataType, isNullable, defaultVal)
    }
}