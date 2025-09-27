package main

import (
	"context"
	"log"
	"net"

	"github.com/dgraph-io/badger/v3"
	"google.golang.org/grpc"

	// TODO: Replace this with the actual import path for your generated protobuf code.
	// You will need to generate this from your .proto file.
	// Example: pb "github.com/your-username/cerberus/api/v1"
	pb "github.com/Frex22/cerberus/api/v1"
)

// server struct will hold everything our server needs, like a reference to the database.
// It must also embed the generated UnimplementedKeyValueStoreServer type to satisfy the gRPC interface.
type server struct {
	// TODO: Add the Unimplemented server type here.
	pb.UnimplementedKeyValueStoreServer
	// Example: pb.UnimplementedKeyValueStoreServer
	// This will hold our BadgerDB instance.

	// TODO: Add a field to hold your BadgerDB instance.
	db *badger.DB
	// Example: db *badger.DB
}

// Put is the implementation of the Put RPC defined in our protobuf.
func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Received Put request for key: %s", req.Key)

	// A BadgerDB transaction can be read-write (Update) or read-only (View).
	// Since we are writing, we need to use db.Update(...).
	err := s.db.Update(func(txn *badger.Txn) error {
		// TODO: Inside the transaction, set the key and value.
		return txn.Set([]byte(req.Key), req.Value)
		// Hint: The function is txn.Set(key, value).
		// Remember to convert the string key and bytes value to byte slices.
		// Example: return txn.Set([]byte(req.Key), req.Value)

	})

	if err != nil {
		log.Printf("Failed to put key %s: %v", req.Key, err)
		return nil, err
	}

	// TODO: Return the correct PutResponse, indicating success.
	return &pb.PutResponse{Success: true}, nil
}

// Get is the implementation of the Get RPC.
func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Received Get request for key: %s", req.Key)
	var valCopy []byte
	var found bool = false

	// Since we are only reading, we use a read-only transaction: db.View(...).
	err := s.db.View(func(txn *badger.Txn) error {
		// TODO: Get the item from the database using the key.
		// Hint: The function is txn.Get(key).
		// Example: item, err := txn.Get([]byte(req.Key))

		item, err := txn.Get([]byte(req.Key))

		// TODO: Handle the case where the key is not found.
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		// BadgerDB returns badger.ErrKeyNotFound when the key doesn't exist.
		// If err is badger.ErrKeyNotFound, it's not a "real" error, it just means no value.

		// TODO: If the key IS found, copy the value.
		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		// Hint: Use item.ValueCopy(nil) to get a safe copy of the value bytes.
		// Set your 'found' variable to true here.
		found = true

		return nil
	})

	if err != nil {
		log.Printf("Failed to get key %s: %v", req.Key, err)
		return nil, err
	}

	// TODO: Return the GetResponse with the value and the 'found' status.
	return &pb.GetResponse{
		Value: valCopy,
		Found: found,
	}, nil
}

// Delete is the implementation of the Delete RPC.
func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Printf("Received Delete request for key: %s", req.Key)

	// TODO: Implement the delete logic. It's a write operation, so use db.Update.
	err := s.db.Update(func(txn *badger.Txn) error {
		// TODO: Call txn.Delete with the key.
		return txn.Delete([]byte(req.Key))
	})

	if err != nil {
		log.Printf("Failed to delete key %s: %v", req.Key, err)
		return nil, err
	}

	// TODO: Return the appropriate DeleteResponse.
	return &pb.DeleteResponse{Success: true}, nil
}

func main() {
	log.Println("Starting Cerberus KV Store Node...")

	// --- 1. Initialize BadgerDB ---
	// TODO: Open a BadgerDB database.
	db, err := badger.Open(badger.DefaultOptions("/tmp/cerberus-data"))
	// Use badger.DefaultOptions with a directory path like "./badger".

	// Example: db, err := badger.Open(badger.DefaultOptions("./badger"))
	// Don't forget to handle the error and defer the db.Close().
	if err != nil {
		log.Fatalf("Failed to open BadgerDB: %v", err)
	}
	defer db.Close()

	// --- 2. Setup TCP Listener ---
	// TODO: Listen on a TCP port (e.g., ":50051").
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	// Example: lis, err := net.Listen("tcp", ":50051")

	// --- 3. Create and Register gRPC Server ---
	// TODO: Create a new gRPC server instance.
	grpcServer := grpc.NewServer()
	// Example: grpcServer := grpc.NewServer()

	// TODO: Create an instance of your server struct.
	s := &server{db: db}
	// Make sure to pass the BadgerDB instance to it.
	// Example: s := &server{db: db}

	// TODO: Register your server implementation with the gRPC server.
	pb.RegisterKeyValueStoreServer(grpcServer, s)
	// The function for this will be in your generated protobuf code.
	// Example: pb.RegisterKeyValueStoreServer(grpcServer, s)

	log.Println("Server is ready and listening on port 50051...")

	// --- 4. Start Serving ---
	// TODO: Start the gRPC server. This is a blocking call.
	// Example: if err := grpcServer.Serve(lis); err != nil { ... }
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
