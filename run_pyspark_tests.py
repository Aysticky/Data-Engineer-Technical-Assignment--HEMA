import sys
import subprocess
from pathlib import Path

# Define test scripts
TESTS = [
    ('Silver Layer', 'tests/local_test_silver_pyspark.py'),
    ('Gold Sales', 'tests/local_test_gold_sales_pyspark.py'),
    ('Gold Customer', 'tests/local_test_gold_customer_pyspark.py'),
]

def check_pyspark():
    # Check if pyspark is installed
    try:
        import pyspark
        version = pyspark.__version__
        
        # Check for known compatibility issues
        import sys
        python_version = sys.version_info
        if python_version >= (3, 12) and version.startswith('3.3.0'):
            print("PySpark 3.3.0 may have compatibility issues")
            
        return True
    except ImportError:
        print("PySpark is not installed")
       
        return False

def check_java():
    # Check if Java is installed
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, 
                              text=True, 
                              timeout=5)
        if result.returncode == 0:
            # Java version is printed to stderr
            version_line = result.stderr.split('\n')[0] if result.stderr else ''
            print(f"Java is installed: {version_line}")
            return True
        else:
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("Java is not installed or not in PATH")
        
        return False

def run_test(name, script_path):
    # Run a single test script
    print(f"Running: {name}")
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            timeout=300  # 5 minutes timeout
        )
        
        if result.returncode == 0:
            print(f"\n{name} test passed")
            return True
        else:
            print(f"\n{name} test failed with exit code {result.returncode}")
            return False
    except subprocess.TimeoutExpired:
        print(f"\n{name} test timed out (exceeded 5 minutes)")
        return False
    except Exception as e:
        print(f"\n{name} test error: {e}")
        return False

def main():
    
    # Check prerequisites
    if not check_java():
        return 1
    
    if not check_pyspark():
        return 1
       
    # Run tests
    results = []
    for name, script_path in TESTS:
        success = run_test(name, script_path)
        results.append((name, success))
    
    # Summary   
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for name, success in results:
        status = "Pass" if success else "Fail"
        print(f"{status}: {name}")
    
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("\nAll PySpark tests passed, and ready for AWS glue deployment")
        return 0
    else:
        print(f"\n{total - passed} test failed. Fix issues before deployment.")
        return 1

if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        sys.exit(130)
