# คู่มือ Deploy โปรเจกต์

โปรเจกต์นี้รองรับการ deploy 2 แบบ:
- Push ไป GitHub (ค่าเริ่มต้น)
- Deploy ไป Google Apps Script

## เริ่มใช้งานเร็ว

เปิดเทอร์มินัลที่โฟลเดอร์โปรเจกต์ แล้วรัน:

```powershell
npm.cmd run deploy
```

คำสั่งนี้จะทำงานดังนี้:
1. Stage ไฟล์ที่มีการเปลี่ยนแปลงทั้งหมด
2. สร้าง commit (ถ้ามีการเปลี่ยนแปลง)
3. Push ไปที่ origin/main

## คำสั่งที่ใช้

```powershell
npm.cmd run deploy
```
Push การเปลี่ยนแปลงล่าสุดขึ้น GitHub (origin/main)

```powershell
npm.cmd run deploy:prod
```
ทำงานเหมือน deploy แต่ใช้ prefix ของ commit message เป็น prod-backup

```powershell
npm.cmd run deploy:gas
```
Deploy ไป Google Apps Script ผ่านสคริปต์ clasp

## เช็กลิสต์ตั้งค่าครั้งแรก

1. มี Git remote ชื่อ origin แล้ว
2. เพิ่ม SSH key ในบัญชี GitHub แล้ว
3. รัน npm install แล้ว

## ตรวจสอบการเชื่อมต่อ GitHub

```powershell
ssh -T git@github.com
```

ถ้าสำเร็จ จะเห็นข้อความลักษณะนี้:
Hi <username>! You've successfully authenticated...

## ปัญหาที่พบบ่อย

### Permission denied (publickey)
ยังไม่ได้เพิ่ม SSH key ในบัญชี GitHub

### No such remote 'origin'
ให้ตั้งค่า remote URL ด้วยคำสั่ง:

```powershell
git remote add origin git@github.com:<user>/<repo>.git
```

### Author identity unknown
ให้ตั้งค่า git identity ใน repo นี้:

```powershell
git config user.name "your-name"
git config user.email "your-email@example.com"
```

## ไฟล์ที่เกี่ยวข้องกับ Deploy

- package.json
- scripts/push-github.ps1
- scripts/deploy.ps1
