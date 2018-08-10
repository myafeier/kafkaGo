package main

import (
	"github.com/Shopify/sarama/mocks"
	"testing"
)

func TestCollectSuccessfully(t *testing.T) {

	dateCollectorMock := mocks.NewSyncProducer(t, nil)
	dateCollectorMock.ExpectSendMessageAndSucceed()

}
