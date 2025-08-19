import streamlit as st
from spark_job.execution_manager import ExecutionManager

st.markdown("<h1 style='text-align:center; background-color:blueviolet; color:yellow;border-radius: 20px;'>DAS User Interface</h1>", unsafe_allow_html=True)

manager = ExecutionManager()
manager.run_jobs()
