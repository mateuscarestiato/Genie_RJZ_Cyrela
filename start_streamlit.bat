@echo off
REM Inicia o app Streamlit (Windows - cmd)
pushd "%~dp0"

echo Iniciando Genie Streamlit...
if exist ".venv\Scripts\activate.bat" (
  echo Ativando virtualenv...
  call ".venv\Scripts\activate.bat"
) else (
  echo Ativacao .venv\\Scripts\\activate.bat nao encontrada.
  echo Se necessario, ative seu virtualenv manualmente antes de rodar este script.
)

echo Lançando Streamlit...
python -m streamlit run "%~dp0genie_web_app.py" %*

popd
exit /b %ERRORLEVEL%
