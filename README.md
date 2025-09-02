# GridDFS - Distributed File System

## 🚀 Quick Start (Windows)

### Prerequisites
- [Git for Windows](https://git-scm.com/download/win)  
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with **WSL2 enabled** on Windows 10/11)  

### Setup
Open **PowerShell** (recommended) and run:

```powershell
# Clone the repository
git clone https://github.com/Yatogami201/griddfs-project.git
cd griddfs-project

# Create shared data directory
mkdir data

# Build and start the system
# Inside the project main folder using command line
docker compose up --build
