const { test, expect } = require('@playwright/test');

test.describe('Core UI reliability flows', () => {
  test('header never overlaps main content', async ({ page }) => {
    await page.goto('/');
    await page.getByLabel('ID Card').fill('STU-001');
    await page.getByLabel('Password').fill('Test1234!');
    await page.getByRole('button', { name: 'Sign In' }).click();
    await expect(page.getByText('Total Inventory Value')).toBeVisible();

    const header = page.getByTestId('app-header');
    const main = page.getByTestId('app-main');
    await expect(header).toBeVisible();
    await expect(main).toBeVisible();

    const headerBox = await header.boundingBox();
    const mainBox = await main.boundingBox();
    expect(headerBox).not.toBeNull();
    expect(mainBox).not.toBeNull();
    expect(mainBox.y).toBeGreaterThanOrEqual(headerBox.y + headerBox.height - 1);

    await page.getByRole('button', { name: 'Issue' }).click();
    await expect(page.getByText('Issue by Scan')).toBeVisible();
    const headerBoxAfter = await header.boundingBox();
    const mainBoxAfter = await main.boundingBox();
    expect(headerBoxAfter).not.toBeNull();
    expect(mainBoxAfter).not.toBeNull();
    expect(mainBoxAfter.y).toBeGreaterThanOrEqual(headerBoxAfter.y + headerBoxAfter.height - 1);
  });

  test('staff can login, check-in, and stock-in scan', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByText('CrimsonSupply Nexus')).toBeVisible();

    await page.getByLabel('ID Card').fill('STU-001');
    await page.getByLabel('Password').fill('Test1234!');
    await page.getByRole('button', { name: 'Sign In' }).click();

    await expect(page.getByText('Total Inventory Value')).toBeVisible();

    await page.getByRole('button', { name: 'Check-In' }).click();
    await expect(page.getByText('Encounter Check-In')).toBeVisible();

    const appointmentId = `E2E-APT-${Date.now()}`;
    await page.getByLabel('Appointment ID').fill(appointmentId);
    await page.getByLabel('Provider ID Card').fill('STU-001');
    await page.getByRole('button', { name: 'Check In' }).click();
    await expect(page.getByText('created.')).toBeVisible();

    await page.getByRole('button', { name: 'Stock In' }).click();
    await expect(page.getByText('Stock In by Scan')).toBeVisible();
    await page.getByLabel('Item Barcode').fill(`E2E-SKU-${Date.now()}`);
    await page.getByLabel('Item Name').fill('E2E Stock Item');
    await page.getByLabel('Quantity').fill('4');
    await page.getByRole('button', { name: 'Stock In' }).click();
    await expect(page.getByText('Stock-in scan recorded.')).toBeVisible();
  });

  test('navigate across 10 routes and first heading is never covered by header', async ({ page }) => {
    await page.goto('/');
    await page.getByLabel('ID Card').fill('STU-001');
    await page.getByLabel('Password').fill('Test1234!');
    await page.getByRole('button', { name: 'Sign In' }).click();
    await expect(page.getByTestId('app-header')).toBeVisible();

    const sequence = [
      { nav: 'Dashboard', heading: 'Dashboard' },
      { nav: 'Check-In', heading: 'Check-In' },
      { nav: 'Stock In', heading: 'Stock In' },
      { nav: 'Issue', heading: 'Issue' },
      { nav: 'Return', heading: 'Return' },
      { nav: 'Cycle Count', heading: 'Cycle Count' },
      { nav: 'Print Queue', heading: 'Print Queue' },
      { nav: 'AI Assistant', heading: 'AI Assistant' },
      { nav: 'Dashboard', heading: 'Dashboard' },
      { nav: 'Issue', heading: 'Issue' },
    ];

    for (const step of sequence) {
      await page.getByRole('button', { name: step.nav }).click();
      const pageHeading = page.getByTestId('page-primary-heading');
      await expect(pageHeading).toHaveText(step.heading);
      const headerBox = await page.getByTestId('app-header').boundingBox();
      const headingBox = await pageHeading.boundingBox();
      expect(headerBox).not.toBeNull();
      expect(headingBox).not.toBeNull();
      expect(headingBox.y).toBeGreaterThanOrEqual(headerBox.y + headerBox.height - 1);
    }
  });
});
