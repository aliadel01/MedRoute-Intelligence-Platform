import time
import random
import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection configuration
DB_CONFIG = {
    "dbname": "medroute_db",
    "user": "debezium_user",
    "password": "debezium_pass",
    "host": "medroute_postgres",
    "port": "5432"
}

# Simulation Control Constants
CHANCE_OF_EVENT = 0.50  # 50% chance that an action happens in each loop
MIN_SLEEP_TIME = 5.0    # At least 5 seconds between loops
MAX_SLEEP_TIME = 15.0    # Up to 15 seconds max sleep time

def get_hospitals(conn):
    """Fetch all hospitals with their capacities to weight the simulation."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, name, beds, icu_beds 
            FROM hospitals 
            WHERE status = 'OPEN' AND icu_beds IS NOT NULL
        """)
        return cur.fetchall()

def update_icu_beds(conn, hospital_id, change):
    """Apply the increment or decrement to the OLTP database."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE hospitals 
            SET icu_beds = icu_beds + %s 
            WHERE id = %s AND (icu_beds + %s) >= 0
        """, (change, hospital_id, change))
    conn.commit()

def main():
    print("Starting Advanced Realistic MedRoute OLTP Simulator...")
    print(f"Configuration: {CHANCE_OF_EVENT*100}% event chance | Min sleep: {MIN_SLEEP_TIME}s\n")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        
        while True:
            # 1. Check if an event should happen in this iteration
            if random.random() > CHANCE_OF_EVENT:
                print("[Idle] No state change in this interval. Hospitals are stable.")
            else:
                # 2. Fetch active hospitals
                hospitals = get_hospitals(conn)
                if not hospitals:
                    print("No active hospitals found. Retrying in 5 seconds...")
                    time.sleep(5)
                    continue
                
                # 3. Create size-based weights
                weights = [h['beds'] + 1 for h in hospitals]
                selected_hospital = random.choices(hospitals, weights=weights, k=1)[0]
                
                h_id = selected_hospital['id']
                h_name = selected_hospital['name']
                current_icu = selected_hospital['icu_beds']
                
                # 4. Realistic capacity-driven logic
                if current_icu <= 2:
                    change = 1
                elif current_icu >= selected_hospital['beds'] * 0.1:
                    change = -1
                else:
                    change = random.choice([1, -1])
                    
                # 5. Apply the change to PostgreSQL
                update_icu_beds(conn, h_id, change)
                
                action = "Admitted Patient (-1)" if change == -1 else "Released Bed (+1)"
                print(f"[OLTP Event] Hospital: {h_name} (ID: {h_id}) | Action: {action}")
            
            # 6. Pacing: Guaranteed minimum of 5 seconds, up to 15 seconds max
            sleep_duration = random.uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME)
            time.sleep(sleep_duration)
            
    except psycopg2.OperationalError as e:
        print(f"Database connection error: {e}")
    except KeyboardInterrupt:
        print("\nSimulator stopped by user.")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()