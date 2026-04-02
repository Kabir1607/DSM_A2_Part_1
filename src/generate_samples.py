import tarfile
import json

def create_samples_from_tar(tar_path, num_entries=1):
    # The EXACT file names from your tar command output
    target_files = [
        'yelp_academic_dataset_business.json',
        'yelp_academic_dataset_checkin.json',
        'yelp_academic_dataset_review.json',
        'yelp_academic_dataset_tip.json',
        'yelp_academic_dataset_user.json'
    ]
    
    print(f"Opening archive: {tar_path} (This might take a few seconds)...")
    
    try:
        with tarfile.open(tar_path, 'r') as tar:
            for target in target_files:
                try:
                    print(f"Extracting {num_entries} lines from {target}...")
                    # Get the specific file directly using its exact name
                    member = tar.getmember(target)
                    f = tar.extractfile(member)
                    
                    # Make a cleaner output name (e.g., sample_business.json)
                    clean_name = target.replace('yelp_academic_dataset_', 'sample_')
                    
                    with open(clean_name, 'w', encoding='utf-8') as outfile:
                        outfile.write("[\n") 
                        
                        for i in range(num_entries):
                            line = f.readline()
                            if not line:
                                break 
                            
                            data = json.loads(line.decode('utf-8'))
                            json.dump(data, outfile, indent=4)
                            
                            if i < num_entries - 1:
                                outfile.write(",\n")
                            else:
                                outfile.write("\n")
                                
                        outfile.write("]\n") 
                        
                    print(f"  Saved to {clean_name}")
                except KeyError:
                    print(f" Could not find {target} in the archive.")
                    
        print("\nAll done! You can now open the sample files in your code editor.")
                
    except Exception as e:
        print(f" An error occurred: {e}")

# Run it
create_samples_from_tar('yelp_dataset.tar', 1)