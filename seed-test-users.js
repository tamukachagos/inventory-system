const bcrypt = require('bcryptjs');
const { Pool } = require('pg');

if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

const required = ['DB_USER', 'DB_HOST', 'DB_NAME', 'DB_PASSWORD', 'DB_PORT'];
for (const key of required) {
  if (!process.env[key]) {
    throw new Error(`Missing required env var: ${key}`);
  }
}

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: Number(process.env.DB_PORT),
  ssl: false,
});

const upsert = async ({ studentCard, name, role, hash }) => {
  await pool.query(
    `INSERT INTO users
       (student_card, name, role, password_hash, is_active, must_reset_password, password_changed_at, password_expires_at)
     VALUES
       ($1, $2, $3, $4, TRUE, FALSE, NOW(), NOW() + INTERVAL '90 days')
     ON CONFLICT (student_card)
     DO UPDATE SET
       name = EXCLUDED.name,
       role = EXCLUDED.role,
       password_hash = EXCLUDED.password_hash,
       is_active = TRUE,
       must_reset_password = FALSE,
       password_changed_at = NOW(),
       password_expires_at = NOW() + INTERVAL '90 days'`,
    [studentCard, name, role, hash]
  );
};

const run = async () => {
  const adminHash = await bcrypt.hash('Admin1234!', 10);
  const staffHash = await bcrypt.hash('Test1234!', 10);
  await upsert({ studentCard: 'ADM-001', name: 'Admin User', role: 'ADMIN', hash: adminHash });
  await upsert({ studentCard: 'STU-001', name: 'Test Staff', role: 'STAFF', hash: staffHash });
  console.log('Seeded test users (ADM-001 / STU-001).');
};

run()
  .then(() => pool.end())
  .catch(async (err) => {
    console.error('Seeding failed:', err && err.message ? err.message : err);
    await pool.end();
    process.exitCode = 1;
  });
