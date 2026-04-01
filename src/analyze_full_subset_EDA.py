import tarfile
import json
from collections import defaultdict

def analyze_businesses_and_users(tar_path):
    # Dictionaries to store our data in memory
    business_to_state = {}
    state_business_count = defaultdict(int)
    state_users = defaultdict(set) # A 'set' automatically prevents duplicate users
    
    print("Phase 1: Mapping businesses to states... (Takes ~10-20 seconds)")
    try:
        with tarfile.open(tar_path, 'r') as tar:
            
            # --- PASS 1: BUSINESSES ---
            b_member = tar.getmember('yelp_academic_dataset_business.json')
            b_file = tar.extractfile(b_member)
            
            for line in b_file:
                data = json.loads(line.decode('utf-8'))
                b_id = data.get('business_id')
                state = data.get('state', 'UNKNOWN')
                
                # Link the business to the state and count it
                business_to_state[b_id] = state
                state_business_count[state] += 1
                
            print(f"  -> Successfully mapped {len(business_to_state):,} businesses.")
            
            # --- PASS 2: REVIEWS (Connecting the Users) ---
            print("\nPhase 2: Connecting users via reviews... (Takes 1-3 minutes)")
            r_member = tar.getmember('yelp_academic_dataset_review.json')
            r_file = tar.extractfile(r_member)
            
            for i, line in enumerate(r_file):
                # Print a progress update every 1 million reviews so you know it's not frozen
                if i > 0 and i % 1000000 == 0:
                    print(f"  -> Processed {i:,} reviews...")
                    
                data = json.loads(line.decode('utf-8'))
                b_id = data.get('business_id')
                u_id = data.get('user_id')
                
                # Look up which state this business is in
                state = business_to_state.get(b_id)
                if state:
                    # Add the user to that state's unique set
                    state_users[state].add(u_id)
                    
        # --- RESULTS SUMMARY ---
        print("\n" + "="*50)
        print(" TOP 15 STATES BY BUSINESS & USER VOLUME")
        print("="*50)
        
        # Sort states by the number of businesses they have (highest to lowest)
        sorted_states = sorted(state_business_count.items(), key=lambda x: x[1], reverse=True)[:15]
        
        print(f"{'State':<7} | {'Businesses':<12} | {'Unique Users':<12}")
        print("-" * 37)
        for state, b_count in sorted_states:
            u_count = len(state_users[state])
            print(f"{state:<7} | {b_count:<12,} | {u_count:<12,}")

    except Exception as e:
        print(f" An error occurred: {e}")

# Run the function
analyze_businesses_and_users('yelp_dataset.tar')