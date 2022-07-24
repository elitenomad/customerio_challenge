package datastore

import (
	"testing"

	"github.com/customerio/homework/serve"
	"github.com/customerio/homework/stream"
)

func TestTotalCustomers(t *testing.T) {
	var datastore = Datastore{
		Customers: map[int]*serve.Customer{
			1: mockCustomer1,
			2: mockCustomer2,
		},
	}

	expected := 2
	count, err := datastore.TotalCustomers()
	if err != nil {
		t.Errorf("error processing data: %v", err)
	}

	if count != expected {
		t.Errorf("customers does not match:\nwant: %#v\nhave: %#v", count, expected)
	}
}

func TestGetIfExists(t *testing.T) {
	var datastore = Datastore{
		Customers: map[int]*serve.Customer{
			1: mockCustomer1,
			2: mockCustomer2,
		},
	}

	customer, err := datastore.Get(1)
	if err != nil {
		t.Errorf("error processing data: %v", err)
	}

	if customer.ID != 1 {
		t.Errorf("customer does not match:\nwant: %#v\nhave: %#v", customer.ID, 1)
	}

	countOfAttributes := len(customer.Attributes)
	if countOfAttributes != 4 {
		t.Errorf("attributes count does not match:\nwant: %#v\nhave: %#v", countOfAttributes, 4)
	}

	countOfEvents := len(customer.Events)
	if countOfEvents != 1 {
		t.Errorf("events count does not match:\nwant: %#v\nhave: %#v", countOfEvents, 1)
	}
}

func TestGetIfNotExists(t *testing.T) {
	var datastore = Datastore{
		Customers: map[int]*serve.Customer{
			1: mockCustomer1,
			2: mockCustomer2,
		},
	}

	customer, err := datastore.Get(3) // ID 3 donot exists
	if err == nil {
		t.Errorf("is expected to raise error not found : %#v", "not found")
	}

	if customer != nil {
		t.Errorf("customer is expected to return nil")
	}
}

func TestCreate(t *testing.T) {
	var datastore = Datastore{
		Customers: map[int]*serve.Customer{
			1: mockCustomer1,
			2: mockCustomer2,
		},
	}

	expected := 3
	attributes := map[string]string{
		"email":  "customer3@example.com",
		"tier":   "C",
		"type":   "permanent",
		"animal": "tiger",
	}

	_, err := datastore.Create(3, attributes)
	if err != nil {
		t.Errorf("error processing data: %v", err)
	}

	count, err := datastore.TotalCustomers()
	if count != expected {
		t.Errorf("customers  count does not match:\nwant: %#v\nhave: %#v", count, expected)
	}
}

func TestCreateIfExists(t *testing.T) {
	var datastore = Datastore{
		Customers: map[int]*serve.Customer{
			1: mockCustomer1,
			2: mockCustomer2,
		},
	}

	expected := mockCustomer1
	attributes := map[string]string{
		"email":  "customer3@example.com",
		"tier":   "C",
		"type":   "permanent",
		"animal": "tiger",
	}

	customer, _ := datastore.Create(1, attributes)
	if customer != expected {
		t.Errorf("customers does not match:\nwant: %#v\nhave: %#v", customer, expected)
	}
}

func TestDelete(t *testing.T) {
	var datastore = Datastore{
		Customers: map[int]*serve.Customer{
			1: mockCustomer1,
			2: mockCustomer2,
		},
	}

	err := datastore.Delete(1)
	if err != nil {
		t.Errorf("error deleting data: %v", err)
	}

	expected := 1
	if len(datastore.Customers) != expected {
		t.Errorf("customers count does not match:\nwant: %#v\nhave: %#v", len(datastore.Customers), expected)
	}
}

func TestDeleteIfNotExists(t *testing.T) {
	var datastore = Datastore{
		Customers: map[int]*serve.Customer{
			1: mockCustomer1,
			2: mockCustomer2,
		},
	}

	err := datastore.Delete(3)
	if err == nil {
		t.Errorf("is expected to raise error not found:\nwant: %#v\nhave: %#v", err, "not found")
	}
}

func TestDeleteAttribute(t *testing.T) {
	var datastore = Datastore{
		Customers: map[int]*serve.Customer{
			1: mockCustomer1,
			2: mockCustomer2,
		},
	}

	customer, err := datastore.DeleteAttribute(1, "animal")
	if err != nil {
		t.Errorf("error deleting data: %v", err)
	}

	expected := 3
	if len(customer.Attributes) != expected {
		t.Errorf("attributes count does not match:\nwant: %#v\nhave: %#v", len(datastore.Customers), expected)
	}
}

func TestGroupEventsByNamePerUser(t *testing.T) {
	// GroupEventsByNamePerUser is called when type is event
	var records = []*stream.Record{
		{
			ID:     "123-456-789",
			Type:   "event",
			Name:   "played_song",
			UserID: "2",
			Data: map[string]string{
				"email":  "customer1@example.com",
				"tier":   "S",
				"type":   "temporary",
				"animal": "tiger",
			},
			Timestamp: 1234567890,
		},
		{
			ID:     "123-456-789",
			Type:   "event",
			Name:   "played_song",
			UserID: "2",
			Data: map[string]string{
				"random": "test",
			},
			Timestamp: 1234567890,
		},
	}

	var datastore = Datastore{
		Customers: map[int]*serve.Customer{
			1: mockCustomer1,
			2: mockCustomer2,
		},
	}

	datastore.GroupEventsByNamePerUser(2, records[0])
	datastore.GroupEventsByNamePerUser(2, records[1])

	expected := map[string]int{
		"played_song": 2,
	}

	for k, v := range expected { // Test the equality for UserID: 2
		if datastore.Customers[2].Events[k] != v {
			t.Errorf("events count does not match:\nwant: %#v\nhave: %#v", v, expected)
		}
	}
}

func TestListIfEmpty(t *testing.T) {
	var datastore = Datastore{
		Customers: map[int]*serve.Customer{},
	}

	_, err := datastore.List(1, 25)
	if err == nil {
		t.Errorf("is expected to raise error not found : %#v", "not found")
	}
}
