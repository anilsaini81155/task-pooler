package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB config
var mongoClient *mongo.Client
var taskCollection *mongo.Collection

// Task structure
type Task struct {
	ID       int           `json:"id"`
	Name     string        `bson:"task_name" json:"name"`
	Duration time.Duration `bson:"duration" json:"duration"` // in seconds
}

// MongoDB model for raw task (before converting duration to time.Duration)
type DBTask struct {
	TaskName string `bson:"task_name"`
	Duration int    `bson:"duration"` // duration in seconds
}

var taskQueue = make(chan Task, 100)
var workersCount = 5
var taskStatus = make(map[int]string)
var taskID = 1

func main() {
	// Connect to MongoDB
	initMongoDB()

	r := gin.Default()

	// Start worker pool
	initWorkers()

	r.GET("/schedule", scheduleTaskFromDB)
	r.GET("/status", checkStatus)

	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

func initMongoDB() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOptions := options.Client().ApplyURI("mongodb+srv://verifyhire:vh12345@verifyhire.outzf6f.mongodb.net/")

	var err error
	mongoClient, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatalf("MongoDB connection error: %v", err)
	}

	taskCollection = mongoClient.Database("companyDB").Collection("tasks")
}

// GET /schedule
func scheduleTaskFromDB(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cursor, err := taskCollection.Find(ctx, bson.D{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch tasks from DB"})
		return
	}
	defer cursor.Close(ctx)

	var dbTasks []DBTask
	if err := cursor.All(ctx, &dbTasks); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode tasks"})
		return
	}

	scheduled := []string{}

	for _, dbTask := range dbTasks {
		task := Task{
			ID:       taskID,
			Name:     dbTask.TaskName,
			Duration: time.Duration(dbTask.Duration) * time.Second,
		}
		taskQueue <- task
		taskStatus[taskID] = "Scheduled"
		scheduled = append(scheduled, fmt.Sprintf("%s (ID: %d)", task.Name, task.ID))
		taskID++
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Tasks scheduled from DB",
		"tasks":   scheduled,
	})
}

// GET /status
func checkStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"tasks": taskStatus,
	})
}

// Worker logic
func startWorker(id int, taskQueue chan Task) {
	for task := range taskQueue {
		fmt.Printf("Worker %d started task %s (ID: %d)\n", id, task.Name, task.ID)

		ctx, cancel := context.WithTimeout(context.Background(), task.Duration)
		done := make(chan bool)

		// Simulate task processing in a goroutine
		go func() {
			// Fake processing: sleeping for a fixed 10 seconds
			// Replace this with actual long-running processing
			time.Sleep(10 * time.Second)
			done <- true
		}()

		select {
		case <-ctx.Done():
			// Timeout occurred
			fmt.Printf("Worker %d: task %s (ID: %d) timed out\n", id, task.Name, task.ID)
			taskStatus[task.ID] = "Timed out"
		case <-done:
			// Task completed in time
			fmt.Printf("Worker %d completed task %s (ID: %d)\n", id, task.Name, task.ID)
			taskStatus[task.ID] = "Completed"
		}

		cancel() // Free resources
	}
}

func initWorkers() {
	for i := 1; i <= workersCount; i++ {
		go startWorker(i, taskQueue)
	}
}
