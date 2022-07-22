package datastore

import (
	"errors"
	"strconv"

	"github.com/customerio/homework/serve"
	"github.com/customerio/homework/stream"
	"golang.org/x/exp/slices"
)

type Datastore struct {
	Customers map[int]*serve.Customer
}

func (d Datastore) Get(id int) (*serve.Customer, error) {
	h, err := d.Customers[id]
	if !err {
		return nil, errors.New("Not found")
	}

	return h, nil
}

func (d Datastore) List(page, count int) ([]*serve.Customer, error) {
	if len(d.Customers) <= 0 {
		return nil, errors.New("Empty")
	}

	start := (page - 1) * count
	stop := start + count

	if start > len(d.Customers) {
		return nil, errors.New("reached limit")
	}

	if stop > len(d.Customers) {
		stop = len(d.Customers)
	}

	cs := []*serve.Customer{}
	for _, v := range d.Customers {
		cs = append(cs, v)
	}

	return cs[start:stop], nil
}

func (d Datastore) Create(id int, attributes map[string]string) (*serve.Customer, error) {
	if _, exists := d.Customers[id]; exists {
		return nil, errors.New("Already exists")
	} else {
		timestamp, err := strconv.Atoi(attributes["timestamp"])
		if err != nil {
			timestamp = 0
		}

		d.Customers[id] = &serve.Customer{
			ID:          id,
			Attributes:  attributes,
			LastUpdated: timestamp,
			Events:      map[string]int{},
			EventIds:    []string{},
		}
	}

	return d.Customers[id], nil
}

func (d Datastore) Update(id int, attributes map[string]string) (*serve.Customer, error) {
	if customer, exists := d.Customers[id]; exists {
		timestamp, _ := strconv.Atoi(attributes["timestamp"])

		for k, v := range attributes {
			if _, exists := customer.Attributes[k]; !exists || (exists && (timestamp >= customer.LastUpdated)) {
				customer.Attributes[k] = v
			}
		}

		if timestamp > customer.LastUpdated {
			customer.LastUpdated = timestamp
		}
	} else {
		d.Create(id, attributes)
	}

	return d.Customers[id], nil
}

func (d Datastore) Delete(id int) error {
	if _, exists := d.Customers[id]; !exists {
		return errors.New("Not found")
	}

	d.Customers[id] = nil
	delete(d.Customers, id)
	return nil
}

func (d Datastore) TotalCustomers() (int, error) {
	if len(d.Customers) <= 0 {
		return -1, errors.New("Empty")
	}

	return len(d.Customers), nil
}

func (d Datastore) GroupEventsByNamePerUser(id int, record *stream.Record) *serve.Customer {
	name := record.Name

	if customer, exists := d.Customers[id]; exists {
		event := customer.Events

		if _, found := event[name]; found {
			if !slices.Contains(d.Customers[id].EventIds, record.ID) {
				event[name] += 1
			}
		} else {
			event[name] = 1
		}
	} else {
		c := &serve.Customer{
			ID:         id,
			Events:     map[string]int{},
			EventIds:   []string{},
			Attributes: make(map[string]string),
		}

		c.Events[name] = 1
		d.Customers[id] = c
	}

	d.Customers[id].EventIds = append(d.Customers[id].EventIds, record.ID)
	return d.Customers[id]
}
