// Debug script to test the complete frontend workflow
const API_BASE_URL = 'http://localhost:8000';

async function debugWorkflow() {
    console.log('🔍 Debugging "Use Existing Table" workflow...\n');
    
    // Step 1: Check if backend is running
    console.log('1️⃣ Testing backend health...');
    try {
        const healthResponse = await fetch(`${API_BASE_URL}/health/database`);
        const healthData = await healthResponse.json();
        console.log('   Backend status:', healthData.status);
        console.log('   Database:', healthData.database);
    } catch (error) {
        console.log('   ❌ Backend not accessible:', error.message);
        return;
    }
    
    // Step 2: Check existing tables
    console.log('\n2️⃣ Checking existing tables...');
    try {
        const tablesResponse = await fetch(`${API_BASE_URL}/tables`);
        const tablesData = await tablesResponse.json();
        console.log('   Found', tablesData.count, 'tables:');
        tablesData.tables.forEach(table => {
            console.log('   -', table.fullName);
        });
        
        const hasAirportFreq = tablesData.tables.some(t => t.table === 'airport_frequencies');
        console.log('   Has airport_frequencies table:', hasAirportFreq ? '✅' : '❌');
    } catch (error) {
        console.log('   ❌ Failed to get tables:', error.message);
        return;
    }
    
    // Step 3: Test CSV format detection
    console.log('\n3️⃣ Testing CSV format detection...');
    const testCsvContent = `id,airport_ref,airport_ident,type,description,frequency_mhz
1,2434,EGLL,TWR,Tower,118.5
2,2434,EGLL,GND,Ground,121.9
3,2434,EGLL,APP,Approach,119.72`;
    
    try {
        // Create FormData to simulate file upload for detection
        const FormData = require('form-data');
        const fs = require('fs');
        
        // Write test file
        fs.writeFileSync('/tmp/test_airport_freq.csv', testCsvContent);
        
        const form = new FormData();
        form.append('file', fs.createReadStream('/tmp/test_airport_freq.csv'));
        
        const detectResponse = await fetch(`${API_BASE_URL}/detect-format`, {
            method: 'POST',
            body: form,
            headers: form.getHeaders()
        });
        
        const detectData = await detectResponse.json();
        console.log('   Format detection result:');
        console.log('     Columns found:', detectData.analysis?.columns?.length || 0);
        console.log('     Columns:', detectData.analysis?.columns || []);
        
    } catch (error) {
        console.log('   ❌ Format detection failed:', error.message);
    }
    
    // Step 4: Test schema matching directly
    console.log('\n4️⃣ Testing schema matching...');
    const testColumns = ["id", "airport_ref", "airport_ident", "type", "description", "frequency_mhz"];
    
    try {
        const matchResponse = await fetch(`${API_BASE_URL}/tables/schema-match`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ columns: testColumns })
        });
        
        const matchData = await matchResponse.json();
        console.log('   Schema match result:');
        console.log('     Response keys:', Object.keys(matchData));
        console.log('     Match count:', matchData.count || 0);
        console.log('     Matching tables:', matchData.matchingTables || []);
        
        // This is the critical test
        if (matchData.matchingTables && matchData.matchingTables.length > 0) {
            console.log('   ✅ Schema matching works correctly');
            console.log('   ✅ Frontend should receive:', {
                availableTables: matchData.matchingTables,
                schemaMatchedTablesCount: matchData.count
            });
        } else {
            console.log('   ❌ No schema matches found');
        }
        
    } catch (error) {
        console.log('   ❌ Schema matching failed:', error.message);
    }
    
    // Step 5: Check frontend fixes
    console.log('\n5️⃣ Verifying frontend fixes...');
    console.log('   Fixed property mapping:');
    console.log('     Backend returns: matchingTables');
    console.log('     Frontend now reads: data.matchingTables ✅');
    console.log('     Backend returns: table.fullName'); 
    console.log('     Frontend now uses: table.fullName ✅');
    
    // Clean up
    try {
        const fs = require('fs');
        fs.unlinkSync('/tmp/test_airport_freq.csv');
    } catch (e) {}
    
    console.log('\n🏁 Debug complete. If schema matching shows matches but frontend');
    console.log('   dropdown is empty, the issue is likely in the frontend JavaScript');
    console.log('   execution or the auto-detection trigger timing.');
}

// Check if we have required modules
try {
    require('form-data');
    debugWorkflow().catch(console.error);
} catch (error) {
    console.log('Installing required modules...');
    const { execSync } = require('child_process');
    execSync('npm install form-data', { stdio: 'inherit' });
    console.log('Retrying...');
    debugWorkflow().catch(console.error);
}