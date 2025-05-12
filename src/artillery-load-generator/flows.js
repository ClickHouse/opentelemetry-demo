const { faker } = require('@faker-js/faker');

module.exports = { simulateUser };

function randomDelay(min = 3000, max = 6000) {
  const delay = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise(resolve => setTimeout(resolve, delay));
}

function formatCardNumber(number) {
    return number.replace(/(\d{4})(?=\d)/g, '$1-'); // optional format: 1234-5678-...
}

function generateValidVisaNumber() {
    let number = '4';
    for (let i = 0; i < 14; i++) {
      number += Math.floor(Math.random() * 10);
    }
    const checkDigit = getLuhnCheckDigit(number);
    return formatCardNumber(number + checkDigit);
}

function getLuhnCheckDigit(number) {
    const digits = number.split('').map(Number);
    let sum = 0;
  
    for (let i = 0; i < digits.length; i++) {
      let digit = digits[i];
      if (i % 2 === 0) {  // even index from the left for 16-digit numbers
        digit *= 2;
        if (digit > 9) digit -= 9;
      }
      sum += digit;
    }
  
    const mod = sum % 10;
    return mod === 0 ? '0' : (10 - mod).toString();
}

async function simulateUser(page) {
  const baseUrl = process.env.DEMO_URL || 'http://localhost:8080';
  await page.goto(`${baseUrl}/`);
  await page.waitForSelector('text=Go Shopping', { timeout: 10000 });
  await randomDelay();
  await page.click('text=Go Shopping');

  const loops = Math.floor(Math.random() * 6) + 2;
  for (let i = 0; i < loops; i++) {
    await page.waitForSelector('[data-cy="product-list"] a', { timeout: 10000 });
    await randomDelay();

    const productLinks = await page.$$('[data-cy="product-list"] a');
    const randomProduct = productLinks[Math.floor(Math.random() * productLinks.length)];
    await randomProduct.click();

    await page.waitForSelector('[data-cy="product-quantity"]', { timeout: 10000 });
    await randomDelay();
    const quantity = Math.floor(Math.random() * 10) + 1;
    await page.selectOption('[data-cy="product-quantity"]', `${quantity}`);

    await randomDelay(min=1000, max=2000);
    await page.waitForSelector('[data-cy="product-add-to-cart"]', { timeout: 10000 });
    await page.click('[data-cy="product-add-to-cart"]');

    await randomDelay();
    await page.waitForSelector('button:has-text("Continue Shopping")', { timeout: 10000 });
    await page.click('button:has-text("Continue Shopping")');

    await randomDelay();
    await page.waitForSelector('text=Go Shopping', { timeout: 10000 });
    await page.click('text=Go Shopping');
  }

    // Click cart icon
    await page.waitForSelector('[data-cy="cart-icon"]', { timeout: 10000 });
    await page.click('[data-cy="cart-icon"]');
    await randomDelay();

    // Click "Go to Shopping Cart"
    await page.waitForSelector('[data-cy="cart-go-to-shopping"]', { timeout: 10000 });
    await page.click('[data-cy="cart-go-to-shopping"]');
    await randomDelay();

    // Fill out the checkout form with random data
    await page.fill('#email', faker.internet.email());
    await randomDelay(min=500, max=1000);
    await page.fill('#street_address', faker.location.streetAddress());
    await randomDelay(min=500, max=1000);
    await page.fill('#zip_code', faker.location.zipCode());
    await randomDelay(min=500, max=1000);
    await page.fill('#city', faker.location.city());
    await randomDelay(min=500, max=1000);
    await page.fill('#state', faker.location.state());
    await randomDelay(min=500, max=1000);
    await page.fill('#credit_card_number', generateValidVisaNumber());
    await randomDelay(min=500, max=1000);
    await page.selectOption('#credit_card_expiration_month', `${faker.number.int({ min: 1, max: 12 })}`);
    await randomDelay(min=500, max=1000);
    await page.selectOption('#credit_card_expiration_year', `${faker.number.int({ min: 2025, max: 2030 })}`);
    await randomDelay(min=500, max=1000);
    await page.fill('#credit_card_cvv', `${faker.number.int({ min: 100, max: 999 })}`);
    await randomDelay(min=500, max=700);
    // Place the order
    await page.click('[data-cy="checkout-place-order"]');
    await page.waitForTimeout(10000); // give browser 10s to flush spans
}
