package main

import (
	"context"
	"io"
	"time"

	"github.com/goliatone/go-export/export"
)

// DemoUser represents a user for demo exports.
type DemoUser struct {
	ID        string
	Email     string
	Name      string
	Role      string
	CreatedAt time.Time
}

// DemoProduct represents a product for demo exports.
type DemoProduct struct {
	ID       string
	Name     string
	SKU      string
	Price    float64
	Quantity int
}

// DemoOrder represents an order for demo exports.
type DemoOrder struct {
	ID        string
	Customer  string
	Total     float64
	Status    string
	CreatedAt time.Time
}

var demoUsers = []DemoUser{
	{ID: "usr_001", Email: "alice@example.com", Name: "Alice Johnson", Role: "admin", CreatedAt: time.Now().AddDate(0, -6, 0)},
	{ID: "usr_002", Email: "bob@example.com", Name: "Bob Smith", Role: "user", CreatedAt: time.Now().AddDate(0, -5, 0)},
	{ID: "usr_003", Email: "carol@example.com", Name: "Carol Williams", Role: "user", CreatedAt: time.Now().AddDate(0, -4, 0)},
	{ID: "usr_004", Email: "david@example.com", Name: "David Brown", Role: "editor", CreatedAt: time.Now().AddDate(0, -3, 0)},
	{ID: "usr_005", Email: "eve@example.com", Name: "Eve Davis", Role: "user", CreatedAt: time.Now().AddDate(0, -2, 0)},
	{ID: "usr_006", Email: "frank@example.com", Name: "Frank Miller", Role: "admin", CreatedAt: time.Now().AddDate(0, -1, 0)},
	{ID: "usr_007", Email: "grace@example.com", Name: "Grace Wilson", Role: "user", CreatedAt: time.Now().AddDate(0, 0, -15)},
	{ID: "usr_008", Email: "henry@example.com", Name: "Henry Moore", Role: "editor", CreatedAt: time.Now().AddDate(0, 0, -10)},
	{ID: "usr_009", Email: "iris@example.com", Name: "Iris Taylor", Role: "user", CreatedAt: time.Now().AddDate(0, 0, -5)},
	{ID: "usr_010", Email: "jack@example.com", Name: "Jack Anderson", Role: "user", CreatedAt: time.Now().AddDate(0, 0, -1)},
}

var demoProducts = []DemoProduct{
	{ID: "prod_001", Name: "Wireless Mouse", SKU: "WM-001", Price: 29.99, Quantity: 150},
	{ID: "prod_002", Name: "Mechanical Keyboard", SKU: "MK-002", Price: 89.99, Quantity: 75},
	{ID: "prod_003", Name: "USB-C Hub", SKU: "UH-003", Price: 49.99, Quantity: 200},
	{ID: "prod_004", Name: "Monitor Stand", SKU: "MS-004", Price: 39.99, Quantity: 120},
	{ID: "prod_005", Name: "Webcam HD", SKU: "WC-005", Price: 79.99, Quantity: 90},
	{ID: "prod_006", Name: "Desk Lamp", SKU: "DL-006", Price: 34.99, Quantity: 180},
	{ID: "prod_007", Name: "Headphones", SKU: "HP-007", Price: 149.99, Quantity: 60},
	{ID: "prod_008", Name: "Mouse Pad XL", SKU: "MP-008", Price: 19.99, Quantity: 300},
}

var demoOrders = []DemoOrder{
	{ID: "ord_001", Customer: "Alice Johnson", Total: 119.98, Status: "completed", CreatedAt: time.Now().AddDate(0, 0, -30)},
	{ID: "ord_002", Customer: "Bob Smith", Total: 89.99, Status: "completed", CreatedAt: time.Now().AddDate(0, 0, -28)},
	{ID: "ord_003", Customer: "Carol Williams", Total: 249.97, Status: "completed", CreatedAt: time.Now().AddDate(0, 0, -25)},
	{ID: "ord_004", Customer: "David Brown", Total: 79.99, Status: "shipped", CreatedAt: time.Now().AddDate(0, 0, -20)},
	{ID: "ord_005", Customer: "Eve Davis", Total: 169.98, Status: "shipped", CreatedAt: time.Now().AddDate(0, 0, -15)},
	{ID: "ord_006", Customer: "Frank Miller", Total: 54.98, Status: "processing", CreatedAt: time.Now().AddDate(0, 0, -10)},
	{ID: "ord_007", Customer: "Grace Wilson", Total: 299.97, Status: "processing", CreatedAt: time.Now().AddDate(0, 0, -5)},
	{ID: "ord_008", Customer: "Henry Moore", Total: 129.99, Status: "pending", CreatedAt: time.Now().AddDate(0, 0, -2)},
	{ID: "ord_009", Customer: "Iris Taylor", Total: 89.99, Status: "pending", CreatedAt: time.Now().AddDate(0, 0, -1)},
	{ID: "ord_010", Customer: "Jack Anderson", Total: 199.98, Status: "pending", CreatedAt: time.Now()},
}

// UserIterator streams demo users.
type UserIterator struct {
	users []DemoUser
	idx   int
}

// NewUserIterator creates a new user iterator.
func NewUserIterator() *UserIterator {
	return &UserIterator{users: demoUsers, idx: 0}
}

func (it *UserIterator) Next(ctx context.Context) (export.Row, error) {
	if it.idx >= len(it.users) {
		return nil, io.EOF
	}
	u := it.users[it.idx]
	it.idx++
	return export.Row{u.ID, u.Email, u.Name, u.Role, u.CreatedAt}, nil
}

func (it *UserIterator) Close() error {
	return nil
}

// ProductIterator streams demo products.
type ProductIterator struct {
	products []DemoProduct
	idx      int
}

// NewProductIterator creates a new product iterator.
func NewProductIterator() *ProductIterator {
	return &ProductIterator{products: demoProducts, idx: 0}
}

func (it *ProductIterator) Next(ctx context.Context) (export.Row, error) {
	if it.idx >= len(it.products) {
		return nil, io.EOF
	}
	p := it.products[it.idx]
	it.idx++
	return export.Row{p.ID, p.Name, p.SKU, p.Price, p.Quantity}, nil
}

func (it *ProductIterator) Close() error {
	return nil
}

// OrderIterator streams demo orders.
type OrderIterator struct {
	orders []DemoOrder
	idx    int
}

// NewOrderIterator creates a new order iterator.
func NewOrderIterator() *OrderIterator {
	return &OrderIterator{orders: demoOrders, idx: 0}
}

func (it *OrderIterator) Next(ctx context.Context) (export.Row, error) {
	if it.idx >= len(it.orders) {
		return nil, io.EOF
	}
	o := it.orders[it.idx]
	it.idx++
	return export.Row{o.ID, o.Customer, o.Total, o.Status, o.CreatedAt}, nil
}

func (it *OrderIterator) Close() error {
	return nil
}
