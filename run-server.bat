@echo off
title Mini Redis Server
echo Starting Mini Redis Server on 127.0.0.1:6379...
"%~dp0target\debug\mini-redis-server.exe"
pause
