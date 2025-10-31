#!/usr/bin/env python3

def trigger_action(action_name: str, params: dict = None):
    """
    Placeholder to call an actuator (e.g., stop machine, alert).
    """
    print(f"Triggering action: {action_name} with params: {params}")

if __name__ == "__main__":
    trigger_action("test_stop", {"reason": "demo"})