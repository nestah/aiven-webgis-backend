// Imports
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const multer = require('multer');
const csvParser = require('csv-parser');
const fs = require('fs');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(express.json());

// CORS Configuration
const corsOptions = {
    origin: 'https://gtl-afya.netlify.app', // Frontend URL
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    credentials: true, // Allow cookies or credentials if needed
    optionsSuccessStatus: 204,
};
app.use(cors(corsOptions));

// Configure multer for CSV uploads
const storage = multer.diskStorage({
    destination: 'uploads/',
    filename: (req, file, cb) => {
        cb(null, `${Date.now()}-${file.originalname}`);
    }
});
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
    ssl: {
        rejectUnauthorized: false, // Required for cloud-hosted PostgreSQL
    },
});

// Validation helper functions
const validateRequiredFields = (row) => {
    const requiredFields = ['uid', 'name', 'facility_type'];
    return requiredFields.filter(field => !row[field] || String(row[field]).trim() === '');
};

const checkForDuplicateUIDs = async (data) => {
    const uids = data.map(row => row.uid);
    const uniqueUids = new Set(uids);

    if (uids.length !== uniqueUids.size) {
        const duplicates = uids.filter((uid, index) => uids.indexOf(uid) !== index);
        return {
            hasDuplicates: true,
            duplicates: [...new Set(duplicates)],
        };
    }

    const existingUids = await pool.query(
        'SELECT uid FROM temp_upload WHERE uid = ANY($1)',
        [Array.from(uniqueUids)]
    );

    if (existingUids.rows.length > 0) {
        return {
            hasDuplicates: true,
            duplicates: existingUids.rows.map(row => row.uid),
        };
    }

    return { hasDuplicates: false, duplicates: [] };
};

// API Endpoints
// Fetch all health facilities
app.get('/api/facilities', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM health_facilities');
        res.json(result.rows);
    } catch (err) {
        console.error('Error fetching facilities:', err);
        res.status(500).json({ error: 'Failed to fetch facilities' });
    }
});

// Fetch facility types
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

// Fetch uploaded facilities
app.get('/api/uploaded-facilities', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM temp_upload ORDER BY county');
        res.json(result.rows);
    } catch (err) {
        console.error('Error fetching uploaded facilities:', err);
        res.status(500).json({ error: 'Failed to fetch uploaded facilities' });
    }
});

// Upload and process CSV file
app.post('/api/upload-csv', upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    const filePath = req.file.path;
    const data = [];
    const errors = [];
    let rowNumber = 1;

    try {
        await new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csvParser({ mapValues: ({ value }) => value.trim() }))
                .on('data', (row) => data.push(row))
                .on('end', resolve)
                .on('error', reject);
        });

        const duplicateCheck = await checkForDuplicateUIDs(data);
        if (duplicateCheck.hasDuplicates) {
            return res.status(400).json({
                error: 'Duplicate UIDs detected',
                details: {
                    message: 'The following UIDs already exist in the database or are duplicated in the CSV:',
                    duplicateUIDs: duplicateCheck.duplicates,
                },
            });
        }

        rowNumber = 1;
        for (const row of data) {
            rowNumber++;
            const missingFields = validateRequiredFields(row);
            if (missingFields.length > 0) {
                errors.push(`Row ${rowNumber}: Missing required fields: ${missingFields.join(', ')}`);
            }
        }

        if (errors.length > 0) {
            return res.status(400).json({
                error: 'Validation errors',
                details: errors,
            });
        }

        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            for (const row of data) {
                const columns = Object.keys(row);
                const values = Object.values(row);
                const placeholders = columns.map((_, index) => `$${index + 1}`).join(', ');

                await client.query(
                    `INSERT INTO temp_upload (${columns.join(', ')}) VALUES (${placeholders})`,
                    values
                );
            }

            await client.query('COMMIT');
            res.json({ message: 'CSV data successfully uploaded', rowsProcessed: data.length });
        } catch (err) {
            await client.query('ROLLBACK');
            console.error('Database error:', err);
            res.status(500).json({ error: 'Database error', details: err.message });
        } finally {
            client.release();
        }
    } catch (err) {
        console.error('Error processing CSV:', err);
        res.status(500).json({ error: 'CSV processing error', details: err.message });
    } finally {
        if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);
        }
    }
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error('Server error:', err);
    res.status(500).json({ error: 'Internal server error', details: err.message });
});

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
