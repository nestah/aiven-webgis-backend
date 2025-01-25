// imports
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const multer = require('multer');
const csvParser = require('csv-parser');
const fs = require('fs');
const cors = require('cors');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

// Configure multer for CSV files
const storage = multer.diskStorage({
    destination: 'uploads/',
    filename: (req, file, cb) => {
        cb(null, `${Date.now()}-${file.originalname}`);
    }
});

// CORS configuration
const corsOptions = {
    origin: 'https://gtl-afya.netlify.app', // Allow your frontend's domain
    methods: ['GET', 'POST', 'PUT', 'DELETE'], // Allowed methods
    allowedHeaders: ['Content-Type', 'Authorization'], // Allowed headers
  };
  
  app.use(cors(corsOptions));
  

const upload = multer({
    storage: storage,
    fileFilter: (req, file, cb) => {
        if (file.mimetype === 'text/csv') {
            cb(null, true);
        } else {
            cb(new Error('Only CSV files are allowed'));
        }
    }
});

// PostgreSQL connection
const pool = new Pool({
    user: process.env.PG_USER,
    host: process.env.PG_HOST,
    database: process.env.PG_DATABASE,
    password: process.env.PG_PASSWORD,
    port: process.env.PG_PORT,
    ssl:{
        rejectUnauthorized:false,
    },
});

// Validation helper functions
const validateRequiredFields = (row) => {
    const requiredFields = ['uid', 'name', 'facility_type'];
    const missingFields = requiredFields.filter(field => !row[field] || String(row[field]).trim() === '');
    return missingFields;
};

const checkForDuplicateUIDs = async (data) => {
    const uids = data.map(row => row.uid);
    const uniqueUids = new Set(uids);
    
    if (uids.length !== uniqueUids.size) {
        const duplicates = uids.filter((uid, index) => uids.indexOf(uid) !== index);
        return {
            hasDuplicates: true,
            duplicates: [...new Set(duplicates)]
        };
    }
    
    // Check against database
    const existingUids = await pool.query(
        'SELECT uid FROM temp_upload WHERE uid = ANY($1)',
        [Array.from(uniqueUids)]
    );
    
    if (existingUids.rows.length > 0) {
        return {
            hasDuplicates: true,
            duplicates: existingUids.rows.map(row => row.uid)
        };
    }
    
    return { hasDuplicates: false, duplicates: [] };
};

// API Routes
app.get('/api/facilities', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM health_facilities');
        res.json(result.rows);
    } catch (err) {
        console.error('Error fetching facilities:', err);
        res.status(500).json({ error: 'Failed to fetch facilities' });
    }
});

app.get('/api/facility-types', async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT DISTINCT facility_type FROM health_facilities WHERE facility_type IS NOT NULL'
        );
        res.json(result.rows);
    } catch (err) {
        console.error('Error fetching facility types:', err);
        res.status(500).json({ error: 'Failed to fetch facility types' });
    }
});

app.get('/api/uploaded-facilities', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM temp_upload ORDER BY county');
        res.json(result.rows);
    } catch (err) {
        console.error('Error fetching uploaded facilities:', err);
        res.status(500).json({ error: 'Failed to fetch uploaded facilities' });
    }
});

app.post('/api/upload-csv', upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    const filePath = req.file.path;
    const data = [];
    const errors = [];
    let rowNumber = 1;

    try {
        // Parse CSV file without field validation
        await new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csvParser({
                    mapValues: ({ value }) => value.trim()
                }))
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', resolve)
                .on('error', reject);
        });

        // Check for duplicate UIDs first
        const duplicateCheck = await checkForDuplicateUIDs(data);
        if (duplicateCheck.hasDuplicates) {
            return res.status(400).json({
                error: 'Duplicate UIDs detected',
                details: {
                    message: 'The following UIDs already exist in the database or are duplicated in the CSV:',
                    duplicateUIDs: duplicateCheck.duplicates
                }
            });
        }

        // Validate required fields only if there are no duplicate UIDs
        rowNumber = 1;
        for (const row of data) {
            rowNumber++;
            const missingFields = validateRequiredFields(row);
            if (missingFields.length > 0) {
                errors.push(`Row ${rowNumber}: Missing required fields: ${missingFields.join(', ')}`);
            }
        }

        // If there are validation errors, return them
        if (errors.length > 0) {
            return res.status(400).json({
                error: 'Validation errors',
                details: errors
            });
        }

        // Begin transaction for data insertion if no errors
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            for (const row of data) {
                const columns = Object.keys(row);
                const values = Object.values(row);
                const placeholders = columns.map((_, index) => `$${index + 1}`).join(', ');
                
                const query = `
                    INSERT INTO temp_upload (${columns.join(', ')})
                    VALUES (${placeholders})
                `;

                await client.query(query, values);
            }

            await client.query('COMMIT');
            res.json({
                message: 'CSV data successfully uploaded',
                rowsProcessed: data.length
            });
        } catch (error) {
            await client.query('ROLLBACK');
            console.error('Database error:', error);
            
            if (error.code === '23505') { // Unique violation
                res.status(400).json({
                    error: 'Duplicate entry',
                    details: {
                        message: 'A record with this UID already exists in the database.',
                        constraint: error.constraint
                    }
                });
            } else {
                res.status(500).json({
                    error: 'Database error',
                    details: {
                        message: 'Failed to insert data into database',
                        errorCode: error.code
                    }
                });
            }
        } finally {
            client.release();
        }
    } catch (error) {
        console.error('Error processing CSV:', error);
        res.status(500).json({
            error: 'CSV processing error',
            details: error.message
        });
    } finally {
        if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);
        }
    }
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({
        error: 'Server error',
        details: err.message
    });
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
