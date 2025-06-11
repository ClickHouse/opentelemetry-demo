# Load generator using artillery

## Usage

To run locally, with one session only to see workflow:

```bash
npm install -g artillery@latest
npm install
npx artillery run load-test-example.yaml
```

This will open a browser and navigate the store (assuming its running):

## Step-by-Step Flow

1. Launch and Navigate
The user navigates to the demo site (DEMO_URL or defaults to http://localhost:8080) and clicks the "Go Shopping" button to enter the product catalog.

2. Product Selection Loop

The script selects a random number of products (between 2 and 7 iterations):

- Waits for the product list to load.
- Randomly selects a product.
- Chooses a random quantity (1–10).
- Adds it to the cart.
- Clicks "Continue Shopping" and repeats the process.

3. View Cart and Checkout

After finishing product selection:

- The user clicks the cart icon.
- Proceeds to the shopping cart view.
- Fills out the checkout form with randomized data:
- Email, address, ZIP code, city, state.
- A valid 16-digit Visa credit card number (generated using the Luhn algorithm).
- Random expiration month/year and CVC code.
- Clicks "Place Order" to complete the checkout.

4. Realistic Timing

Randomized delays (3–6 seconds, or shorter where appropriate) are inserted between each interaction to simulate human browsing behavior.
