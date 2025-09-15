#!/usr/bin/env python3
"""
Verificación de estructura de volúmenes
"""
import requests
import os
from config import NAMENODE_URL

def check_volumes():
    print("🔍 Verificando estructura de volúmenes...")
    
    # Check NameNode
    try:
        nn_resp = requests.get(NAMENODE_URL)
        print("✅ NameNode respondiendo")
    except:
        print("❌ NameNode no disponible")
        return False
    
    # Check DataNodes
    datanodes = [
        "http://localhost:5001",
        "http://localhost:5002", 
        "http://localhost:5003"
    ]
    
    for dn_url in datanodes:
        try:
            dn_resp = requests.get(f"{dn_url}/")
            dn_info = dn_resp.json()
            print(f"✅ {dn_url}: {dn_info['total_blocks']} blocks, {dn_info['total_size']} bytes")
            
            # Check storage info
            storage_resp = requests.get(f"{dn_url}/storage_info")
            storage_info = storage_resp.json()
            print(f"   📦 Storage: {storage_info['storage_root']}")
            print(f"   💾 Free: {storage_info['free_space']} bytes")
            
        except:
            print(f"❌ {dn_url}: No disponible")
    
    return True

if __name__ == "__main__":
    check_volumes()