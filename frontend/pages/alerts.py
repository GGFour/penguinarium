# This file is deprecated. Alert functionality has been moved to navigator.py
# Keeping this file for backward compatibility, but all functions are now in navigator.py

import streamlit as st
from navigator import display_alerts_menu, go_to_selected_alert_id, show_alert_list

# Re-export functions for backward compatibility
__all__ = ['display_alerts_menu', 'go_to_selected_alert_id', 'show_alert_list']
