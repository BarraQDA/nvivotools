@echo off
call attach %2 nvivo
DenormaliseNVP.py --windows "sqlite:///%1" mssql+pymssql://nvivotools:nvivotools@%COMPUTERNAME%/nvivo 
call save %2 nvivo
