import json
import sys
import os

def check_regression(current_file, baseline_file, threshold=0.2):
    if not os.path.exists(baseline_file):
        print(f"No baseline found at {baseline_file}. Skipping comparison.")
        return True

    try:
        with open(current_file) as f:
            current = json.load(f)
        with open(baseline_file) as f:
            baseline = json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return True

    regressions = []
    
    # Create map of baseline metrics
    base_map = {b['name']: b['real_time'] for b in baseline['benchmarks']}

    print(f"{'Benchmark':<40} | {'Old (ns)':<12} | {'New (ns)':<12} | {'Change':<10}")
    print("-" * 85)

    for b in current['benchmarks']:
        name = b['name']
        if name in base_map:
            old_time = base_map[name]
            new_time = b['real_time']
            
            # Increase in time means decrease in performance
            change = (new_time - old_time) / old_time
            print(f"{name:<40} | {old_time:<12.2f} | {new_time:<12.2f} | {change:>+9.1%}")
            
            if change > threshold:
                regressions.append(f"{name} regressed by {change:.1%}")

    if regressions:
        print("\n!!! PERFORMANCE REGRESSION DETECTED !!!")
        for r in regressions:
            print(f"  - {r}")
        return False
    
    print("\nPerformance is within acceptable limits.")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: check_perf_regression.py <current.json> <baseline.json> [threshold]")
        sys.exit(1)
    
    thresh = float(sys.argv[3]) if len(sys.argv) > 3 else 0.2
    if not check_regression(sys.argv[1], sys.argv[2], thresh):
        sys.exit(1)
