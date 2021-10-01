package publisher

import (
	"context"
	pb "imc/grpc_psb/protobuf"
	"google.golang.org/grpc/grpclog"
)

func Publish(client pb.PublisherClient, key string, msg *pb.Message) {
	request := &pb.PublishRequest{Key: key, Messages: []*pb.Message{msg}}
	_, error := client.Publish(context.Background(), request)
	if error != nil {
		grpclog.Printf("Error publishing")

	}

}

