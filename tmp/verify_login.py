import requests

BASE_URL = "http://127.0.0.1:7860"

def test_login_flow():
    print("Testing Login Flow...")
    
    # Payload for login
    data = {
        "email": "test@test.com",
        "password": "wrongpassword"
    }
    
    # POST to the new frontend route
    try:
        response = requests.post(
            f"{BASE_URL}/auth/login",
            data=data,
            allow_redirects=False
        )
        
        print(f"Status Code: {response.status_code}")
        print(f"Headers: {response.headers}")
        print(f"Cookies: {response.cookies.get_dict()}")
        
        # We expect a 302 redirect
        if response.status_code == 302:
            location = response.headers.get("Location", "")
            print(f"Redirect Location: {location}")
            
            if "error=1" in location:
                print("SUCCESS: Correctly redirected to login with error=1 (due to wrong credentials)")
            elif location == "/":
                 print("SUCCESS: Correctly redirected to /")
            else:
                print(f"FAILURE: Unexpected redirect location: {location}")
        else:
            print(f"FAILURE: Expected 302, got {response.status_code}")

    except Exception as e:
        print(f"EXCEPTION: {e}")

if __name__ == "__main__":
    test_login_flow()
