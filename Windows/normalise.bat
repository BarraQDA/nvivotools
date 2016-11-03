@echo off
call attach %1 nvivo
NormaliseNVP.py --windows mssql+pymssql://nvivotools:nvivotools@%COMPUTERNAME%/nvivo "sqlite:///%2"
call drop nvivo
