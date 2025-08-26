const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

async function testFrontendWorkflow() {
    console.log('🚀 Starting frontend test...\n');
    
    // Launch browser with security flags
    const browser = await chromium.launch({ 
        headless: false,
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    });
    
    const page = await browser.newPage();
    
    try {
        // Navigate to frontend
        console.log('📍 Navigating to frontend...');
        await page.goto('http://localhost:8080', { waitUntil: 'networkidle' });
        console.log('✅ Frontend loaded\n');
        
        // Wait for Alpine.js to initialize
        await page.waitForTimeout(2000);
        
        // Create test file
        const testFilePath = '/home/lin/repo/reference_data_mgr/airport_frequencies.20250811.csv';
        const testFileContent = `id,airport_ref,airport_ident,type,description,frequency_mhz
1,2434,EGLL,TWR,Tower,118.5
2,2434,EGLL,GND,Ground,121.9
3,2434,EGLL,APP,Approach,119.72`;
        
        fs.writeFileSync(testFilePath, testFileContent);
        
        // Upload file
        console.log('📁 Uploading test file...');
        const fileInput = await page.locator('input[type="file"]');
        await fileInput.setInputFiles(testFilePath);
        console.log('✅ File uploaded\n');
        
        // Wait for auto-detection
        console.log('🔍 Waiting for auto-detection...');
        await page.waitForTimeout(3000);
        
        // Check suggested table name
        const suggestedTableName = await page.locator('input[placeholder*="table name"]').inputValue();
        console.log('📝 Suggested table name:', suggestedTableName);
        
        if (suggestedTableName === 'airport_frequencies') {
            console.log('✅ Table name extraction working correctly');
        } else {
            console.log('❌ Table name extraction failed - expected "airport_frequencies", got:', suggestedTableName);
        }
        
        // Check if auto-detection completed
        const detectionButton = await page.locator('button:has-text("Auto-Detect Format")');
        const isDetecting = await page.locator('text="Detecting..."').count() > 0;
        
        if (isDetecting) {
            console.log('⏳ Still detecting, waiting longer...');
            await page.waitForTimeout(5000);
        }
        
        // Check table option (should switch to 'existing' if matches found)
        const useExistingRadio = await page.locator('input[value="existing"]');
        const isExistingSelected = await useExistingRadio.isChecked();
        
        console.log('🔄 Table option - Use Existing selected:', isExistingSelected);
        
        if (isExistingSelected) {
            console.log('✅ Auto-switched to "Use Existing Table"');
            
            // Check dropdown options
            const dropdown = await page.locator('select').first();
            const options = await dropdown.locator('option').allTextContents();
            
            console.log('📋 Dropdown options:');
            options.forEach((option, index) => {
                console.log(`  ${index}: ${option}`);
            });
            
            const hasAirportFrequencies = options.some(opt => opt.includes('airport_frequencies'));
            if (hasAirportFrequencies) {
                console.log('✅ Found airport_frequencies in dropdown');
            } else {
                console.log('❌ airport_frequencies not found in dropdown');
            }
        } else {
            console.log('❌ Did not auto-switch to existing table');
            
            // Check if there are any console errors
            page.on('console', msg => {
                if (msg.type() === 'error') {
                    console.log('🔴 Console error:', msg.text());
                }
            });
            
            // Manually trigger auto-detection
            console.log('🔧 Manually triggering detection...');
            await detectionButton.click();
            await page.waitForTimeout(3000);
            
            // Check again
            const isNowExistingSelected = await useExistingRadio.isChecked();
            console.log('🔄 After manual detection - Use Existing selected:', isNowExistingSelected);
        }
        
        // Take screenshot for debugging
        await page.screenshot({ path: 'frontend_test_screenshot.png', fullPage: true });
        console.log('📸 Screenshot saved as frontend_test_screenshot.png');
        
    } catch (error) {
        console.error('❌ Test failed:', error);
        await page.screenshot({ path: 'frontend_error_screenshot.png', fullPage: true });
        console.log('📸 Error screenshot saved');
    } finally {
        await browser.close();
        console.log('\n🏁 Test completed');
    }
}

testFrontendWorkflow().catch(console.error);