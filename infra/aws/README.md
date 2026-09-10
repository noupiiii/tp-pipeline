# Déploiement sur AWS EC2

Déploiement simple du pipeline conteneurisé (Docker Compose) sur une instance EC2.

## 1. Provisionner l'instance

- Type recommandé : `t3.large` minimum (Hadoop + Spark + Airflow + Postgres sont gourmands en RAM ; 8 Go RAM mini).
- AMI : Ubuntu 22.04 LTS.
- Security Group : ouvrir en entrée
  - 22 (SSH, restreint à votre IP)
  - 8080 (Airflow UI)
  - 8081 (Spark UI)
  - 9870 (HDFS namenode UI)
  - 5432 (PostgreSQL, uniquement si accès externe nécessaire — sinon garder fermé)

## 2. Installer Docker sur l'instance

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER
```

## 3. Déployer le pipeline

```bash
git clone <url-du-repo> tp-dialo
cd tp-dialo
cp .env.example .env
# éditer .env : mettre un mot de passe PostgreSQL fort, etc.
docker compose up -d
```

## 4. Vérifications post-déploiement

```bash
docker compose ps
curl -s http://localhost:8080/health   # Airflow
curl -s http://localhost:9870          # HDFS namenode UI
```

## Notes

- Ce déploiement manuel convient pour une démo/TP. Pour de la production, préférer
  une infra as code (Terraform) + un registre d'images (ECR) + CI/CD qui build et
  push les images, puis déclenche un `docker compose pull && up -d` distant (via SSH
  action GitHub Actions ou un webhook).
- Les volumes Docker (HDFS, PostgreSQL) sont locaux à l'instance : pour de la
  persistance réelle, monter un volume EBS dédié sur `/var/lib/docker/volumes`.
