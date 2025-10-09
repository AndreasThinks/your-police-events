from playwright.sync_api import sync_playwright, expect

def run_verification():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Navigate to the home page
        page.goto("http://localhost:8000")

        # Wait for the stats element to be visible and contain the expected text
        stats_element = page.locator("#appStats")
        expect(stats_element).to_be_visible()
        # The text will initially be "Loading stats..." and then update.
        # We'll wait for it to contain "visitors", which indicates the API call was successful.
        expect(stats_element).to_contain_text("visitors", timeout=10000) # Increased timeout for safety

        # Take a screenshot of the footer to show the new stats
        footer_element = page.locator("footer")
        footer_element.screenshot(path="jules-scratch/verification/verification.png")

        browser.close()

if __name__ == "__main__":
    run_verification()