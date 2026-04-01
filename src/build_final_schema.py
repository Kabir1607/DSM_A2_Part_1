import json
import os
from collections import defaultdict

DATA_DIR = '/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/extracted_data'

def build_final_businesses():
    print(" Merging Tips and Check-ins into Business documents...")
    
    # 1. Load Tips into memory
    biz_tips = defaultdict(list)
    if os.path.exists(f'{DATA_DIR}/subset_tip.json'):
        with open(f'{DATA_DIR}/subset_tip.json', 'r', encoding='utf-8') as f:
            for line in f:
                tip = json.loads(line)
                b_id = tip.pop('business_id') # Remove business_id as it will be redundant inside the business doc
                biz_tips[b_id].append(tip)
        print(f" Loaded tips for {len(biz_tips):,} businesses.")

    # 2. Load Check-ins into memory
    biz_checkins = {}
    if os.path.exists(f'{DATA_DIR}/subset_checkin.json'):
        with open(f'{DATA_DIR}/subset_checkin.json', 'r', encoding='utf-8') as f:
            for line in f:
                chk = json.loads(line)
                # Yelp stores dates as a single comma-separated string. We split it into a proper array.
                biz_checkins[chk['business_id']] = chk['date'].split(', ')
        print(f" Loaded check-ins for {len(biz_checkins):,} businesses.")

    # 3. Stream Businesses, Embed Data, and Save
    processed = 0
    with open(f'{DATA_DIR}/subset_business.json', 'r', encoding='utf-8') as in_b, \
         open(f'{DATA_DIR}/final_businesses.json', 'w', encoding='utf-8') as out_b:
        
        for line in in_b:
            biz = json.loads(line)
            b_id = biz['business_id']
            
            # Embed the arrays (or empty arrays if none exist)
            biz['tips'] = biz_tips.get(b_id, [])
            biz['checkins'] = biz_checkins.get(b_id, [])
            
            out_b.write(json.dumps(biz) + '\n')
            processed += 1
            
    print(f"Success! Created final_businesses.json with {processed:,} documents.")
    print("You can now safely delete subset_business, subset_tip, and subset_checkin JSON files if you want to save space.")

# Run it
build_final_businesses()