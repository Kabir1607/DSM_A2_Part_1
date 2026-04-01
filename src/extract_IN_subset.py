import tarfile
import json
import os

def extract_subset(tar_path, target_state="IN"):
    print(f" Starting extraction for state: {target_state}")
    print("This will take a few minutes. Please do not close the terminal.\n")
    
    in_businesses = set()
    in_users = set()
    
    try:
        with tarfile.open(tar_path, 'r') as tar:
            
            # ---------------------------------------------------------
            # PASS 1: Extract Businesses
            # ---------------------------------------------------------
            print(f"Phase 1: Extracting {target_state} businesses...")
            b_member = tar.getmember('yelp_academic_dataset_business.json')
            b_file = tar.extractfile(b_member)
            
            with open('subset_business.json', 'w', encoding='utf-8') as out_b:
                for line in b_file:
                    data = json.loads(line.decode('utf-8'))
                    if data.get('state') == target_state:
                        in_businesses.add(data.get('business_id'))
                        out_b.write(line.decode('utf-8'))
                        
            print(f" Found {len(in_businesses):,} businesses in {target_state}.")

            # ---------------------------------------------------------
            # PASS 2: Extract Reviews, Tips, and Checkins
            # ---------------------------------------------------------
            print("\nPhase 2: Extracting connected Reviews, Tips, and Checkins...")
            
            # --- REVIEWS ---
            print(" -> Scanning 7 million reviews (this takes the longest)...")
            r_member = tar.getmember('yelp_academic_dataset_review.json')
            r_file = tar.extractfile(r_member)
            
            reviews_saved = 0
            with open('subset_review.json', 'w', encoding='utf-8') as out_r:
                for line in r_file:
                    data = json.loads(line.decode('utf-8'))
                    if data.get('business_id') in in_businesses:
                        in_users.add(data.get('user_id'))
                        out_r.write(line.decode('utf-8'))
                        reviews_saved += 1
            print(f" Saved {reviews_saved:,} reviews.")

            # --- TIPS ---
            print(" -> Scanning tips...")
            t_member = tar.getmember('yelp_academic_dataset_tip.json')
            t_file = tar.extractfile(t_member)
            
            tips_saved = 0
            with open('subset_tip.json', 'w', encoding='utf-8') as out_t:
                for line in t_file:
                    data = json.loads(line.decode('utf-8'))
                    if data.get('business_id') in in_businesses:
                        in_users.add(data.get('user_id')) # Tips also connect users!
                        out_t.write(line.decode('utf-8'))
                        tips_saved += 1
            print(f" Saved {tips_saved:,} tips.")

            # --- CHECKINS ---
            print(" -> Scanning check-ins...")
            c_member = tar.getmember('yelp_academic_dataset_checkin.json')
            c_file = tar.extractfile(c_member)
            
            checkins_saved = 0
            with open('subset_checkin.json', 'w', encoding='utf-8') as out_c:
                for line in c_file:
                    data = json.loads(line.decode('utf-8'))
                    if data.get('business_id') in in_businesses:
                        out_c.write(line.decode('utf-8'))
                        checkins_saved += 1
            print(f" Saved {checkins_saved:,} check-ins.")

            # ---------------------------------------------------------
            # PASS 3: Extract Users
            # ---------------------------------------------------------
            print(f"\nPhase 3: Extracting {len(in_users):,} connected users...")
            u_member = tar.getmember('yelp_academic_dataset_user.json')
            u_file = tar.extractfile(u_member)
            
            users_saved = 0
            with open('subset_user.json', 'w', encoding='utf-8') as out_u:
                for line in u_file:
                    data = json.loads(line.decode('utf-8'))
                    if data.get('user_id') in in_users:
                        out_u.write(line.decode('utf-8'))
                        users_saved += 1
            print(f" Saved {users_saved:,} user profiles.")

        print("\n Extraction Complete! All subset files are ready for database import.")

    except Exception as e:
        print(f"❌ An error occurred: {e}")

# Run the extraction
extract_subset('/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/data/yelp_dataset.tar')