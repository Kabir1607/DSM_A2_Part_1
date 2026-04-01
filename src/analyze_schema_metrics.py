import json
import os
from collections import defaultdict

def analyze_json_file(filepath, id_field):
    """Calculates document size metrics for a given JSON file."""
    if not os.path.exists(filepath):
        return None
        
    sizes = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            sizes.append(len(line.encode('utf-8')))
            
    if not sizes:
        return None
        
    return {
        'count': len(sizes),
        'avg_bytes': sum(sizes) / len(sizes),
        'max_bytes': max(sizes),
        'total_mb': sum(sizes) / (1024 * 1024)
    }

def analyze_relationships():
    # Open a text file to save our report
    with open('schema_metrics_report.txt', 'w', encoding='utf-8') as report_file:
        
        # Custom print function that writes to both the terminal and the text file
        def log(message=""):
            print(message)
            report_file.write(message + "\n")

        log("🔍 Analyzing schema metrics and relationship cardinality...\n")
        
        # 1. Base Document Sizes
        log("-" * 60)
        log("1. BASE DOCUMENT SIZES (Average / Max)")
        log("-" * 60)
        files_to_check = {
            'Business': ('/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/extracted_data/subset_business.json', 'business_id'),
            'Review': ('/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/extracted_data/subset_review.json', 'review_id'),
            'User': ('/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/extracted_data/subset_user.json', 'user_id'),
            'Tip': ('/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/extracted_data/subset_tip.json', 'text'),
            'Checkin': ('/home/Kdixter/Desktop/School_Stuff/DSM/DSM_A2/extracted_data/subset_checkin.json', 'business_id')
        }
        
        doc_stats = {}
        for name, (filepath, id_field) in files_to_check.items():
            stats = analyze_json_file(filepath, id_field)
            if stats:
                doc_stats[name] = stats
                log(f"{name:<10}: Avg {stats['avg_bytes']:>5.0f} bytes | Max {stats['max_bytes']:>6.0f} bytes | Total {stats['total_mb']:>5.1f} MB")

        # 2. Relationship Cardinality (1:N mapping)
        log("\n" + "-" * 60)
        log("2. RELATIONSHIP CARDINALITY (Business as Parent)")
        log("-" * 60)
        
        biz_reviews = defaultdict(list)
        biz_tips = defaultdict(list)
        
        # Map Reviews to Businesses
        if os.path.exists('subset_review.json'):
            with open('subset_review.json', 'r', encoding='utf-8') as f:
                for line in f:
                    data = json.loads(line)
                    biz_reviews[data['business_id']].append(len(line.encode('utf-8')))
                    
        # Map Tips to Businesses
        if os.path.exists('subset_tip.json'):
            with open('subset_tip.json', 'r', encoding='utf-8') as f:
                for line in f:
                    data = json.loads(line)
                    biz_tips[data['business_id']].append(len(line.encode('utf-8')))

        # Calculate 1:N stats
        max_reviews = max([len(r) for r in biz_reviews.values()]) if biz_reviews else 0
        avg_reviews = sum([len(r) for r in biz_reviews.values()]) / len(biz_reviews) if biz_reviews else 0
        
        max_tips = max([len(t) for t in biz_tips.values()]) if biz_tips else 0
        avg_tips = sum([len(t) for t in biz_tips.values()]) / len(biz_tips) if biz_tips else 0

        log(f"Reviews per Business : Avg {avg_reviews:>5.1f} | Max {max_reviews:,}")
        log(f"Tips per Business    : Avg {avg_tips:>5.1f} | Max {max_tips:,}")

        # 3. The 16MB Limit Projection Test
        log("\n" + "-" * 60)
        log("3. THE 16MB EMBEDDING LIMIT TEST (16,777,216 Bytes)")
        log("-" * 60)
        
        # Find the business with the absolute largest review/tip payload
        max_review_payload_bytes = max([sum(r) for r in biz_reviews.values()]) if biz_reviews else 0
        max_tip_payload_bytes = max([sum(t) for t in biz_tips.values()]) if biz_tips else 0
        
        mb_limit = 16 * 1024 * 1024
        review_pct = (max_review_payload_bytes / mb_limit) * 100
        tip_pct = (max_tip_payload_bytes / mb_limit) * 100
        
        log(f"If we embed ALL REVIEWS into their parent Business:")
        log(f" -> The largest business would add {max_review_payload_bytes:,} bytes ({review_pct:.2f}% of 16MB limit).")
        if review_pct > 80:
            log(" ⚠️ DANGER: Highly unbounded array. Referencing is strongly recommended.")
        else:
            log(" ✅ Safe to embed, but referencing might still be preferred for query performance.")
            
        log(f"\nIf we embed ALL TIPS into their parent Business:")
        log(f" -> The largest business would add {max_tip_payload_bytes:,} bytes ({tip_pct:.2f}% of 16MB limit).")
        if tip_pct > 80:
            log(" ⚠️ DANGER: Unbounded array.")
        else:
            log(" ✅ Safe to embed. Low risk of hitting 16MB limit.")
            
        log("\nReport successfully saved to schema_metrics_report.txt")

# Run it
analyze_relationships()