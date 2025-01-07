import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, List
import re

class VisualizationSelector:
    def __init__(self):
        # SQL-specific keywords and aggregations
        self.sql_aggregations = ['sum', 'count', 'avg', 'average', 'min', 'max', 'group by', 'order by']
        self.sql_time_patterns = ['date_trunc', 'extract', 'datepart', 'year', 'month', 'day', 'week']
        
        # Natural language and business intent keywords
        self.time_keywords = {
            'high': ['trend', 'over time', 'historical', 'timeline'],
            'medium': ['daily', 'monthly', 'yearly', 'period', 'seasonal'],
            'low': ['date', 'time', 'when']
        }
        self.comparison_keywords = {
            'high': ['compare', 'versus', 'vs', 'difference between'],
            'medium': ['against', 'relative to', 'higher than', 'lower than'],
            'low': ['more', 'less', 'greater', 'smaller']
        }
        self.distribution_keywords = {
            'high': ['distribution', 'spread', 'histogram', 'frequency'],
            'medium': ['range', 'variance', 'deviation'],
            'low': ['across', 'among', 'between']
        }
        self.relationship_keywords = {
            'high': ['correlation', 'relationship', 'scatter', 'impact of'],
            'medium': ['affects', 'influences', 'depends on'],
            'low': ['between', 'with']
        }
        self.composition_keywords = {
            'high': ['breakdown', 'composition', 'pie chart', 'percentage of total'],
            'medium': ['share', 'ratio', 'proportion'],
            'low': ['part', 'segment', 'split']
        }
        
        # Chart type indicators
        self.chart_indicators = {
            'line': ['line chart', 'trend line', 'time series'],
            'bar': ['bar chart', 'bar graph', 'histogram'],
            'pie': ['pie chart', 'donut chart', 'pie graph'],
            'scatter': ['scatter plot', 'scatter graph', 'correlation plot'],
            'area': ['area chart', 'stacked area', 'cumulative'],
        }

    def analyze_query_intent(self, query: str) -> Dict[str, float]:
        """Analyze the query text to determine visualization intent with confidence scores."""
        query = query.lower()
        intents = {
            'temporal': 0.0,
            'comparison': 0.0,
            'distribution': 0.0,
            'relationship': 0.0,
            'composition': 0.0
        }
        
        # Check SQL-specific patterns
        sql_temporal_score = sum(pattern in query for pattern in self.sql_time_patterns) * 0.3
        sql_agg_score = sum(agg in query for agg in self.sql_aggregations) * 0.2
        intents['temporal'] += sql_temporal_score
        
        # Analyze keywords with weights
        for intent, keywords in [
            ('temporal', self.time_keywords),
            ('comparison', self.comparison_keywords),
            ('distribution', self.distribution_keywords),
            ('relationship', self.relationship_keywords),
            ('composition', self.composition_keywords)
        ]:
            score = 0.0
            score += sum(1.0 for word in keywords['high'] if word in query)
            score += sum(0.6 for word in keywords['medium'] if word in query)
            score += sum(0.3 for word in keywords['low'] if word in query)
            intents[intent] += score
            
        # Check for explicit chart type mentions
        for chart_type, indicators in self.chart_indicators.items():
            if any(indicator in query for indicator in indicators):
                if chart_type == 'line':
                    intents['temporal'] += 1.0
                elif chart_type == 'bar':
                    intents['comparison'] += 1.0
                elif chart_type == 'pie':
                    intents['composition'] += 1.0
                elif chart_type == 'scatter':
                    intents['relationship'] += 1.0
                    
        # Normalize scores
        max_score = max(intents.values()) if max(intents.values()) > 0 else 1.0
        intents = {k: v/max_score for k, v in intents.items()}
        
        return intents

    def analyze_data_structure(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze the structure of the result DataFrame."""
        return {
            'num_rows': len(df),
            'num_cols': len(df.columns),
            'numeric_cols': df.select_dtypes(include=['int64', 'float64']).columns.tolist(),
            'categorical_cols': df.select_dtypes(include=['object', 'category']).columns.tolist(),
            'datetime_cols': df.select_dtypes(include=['datetime64']).columns.tolist()
        }

    def select_visualization(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Select the most appropriate visualization based on query intent and data structure."""
        intents = self.analyze_query_intent(query)
        structure = self.analyze_data_structure(df)
        
        # Handle single value results
        if structure['num_rows'] == 1 and structure['num_cols'] == 1:
            return {
                'type': 'value',
                'value': df.iloc[0, 0],
                'confidence': 1.0
            }
            
        # Get the primary intent (highest score)
        primary_intent = max(intents.items(), key=lambda x: x[1])
        intent_type, confidence = primary_intent
        
        # Analyze column types
        num_numeric = len(structure['numeric_cols'])
        num_categorical = len(structure['categorical_cols'])
        has_datetime = len(structure['datetime_cols']) > 0
        
        viz_config = {
            'type': None,
            'x': None,
            'y': None,
            'color': None,
            'confidence': confidence
        }
        
        # Select visualization based on intent and data structure
        if intent_type == 'temporal' and has_datetime:
            viz_config['type'] = 'line'
            viz_config['x'] = structure['datetime_cols'][0]
            if num_numeric > 0:
                viz_config['y'] = structure['numeric_cols'][0]
                if num_categorical > 0:
                    viz_config['color'] = structure['categorical_cols'][0]
                    
        elif intent_type == 'comparison':
            if num_categorical > 0 and num_numeric > 0:
                viz_config['type'] = 'bar'
                viz_config['x'] = structure['categorical_cols'][0]
                viz_config['y'] = structure['numeric_cols'][0]
                if len(structure['categorical_cols']) > 1:
                    viz_config['color'] = structure['categorical_cols'][1]
                    
        elif intent_type == 'distribution' and num_numeric > 0:
            if num_categorical > 0:
                viz_config['type'] = 'box'
                viz_config['x'] = structure['categorical_cols'][0]
                viz_config['y'] = structure['numeric_cols'][0]
            else:
                viz_config['type'] = 'histogram'
                viz_config['x'] = structure['numeric_cols'][0]
                
        elif intent_type == 'relationship' and num_numeric >= 2:
            viz_config['type'] = 'scatter'
            viz_config['x'] = structure['numeric_cols'][0]
            viz_config['y'] = structure['numeric_cols'][1]
            if num_categorical > 0:
                viz_config['color'] = structure['categorical_cols'][0]
                
        elif intent_type == 'composition' and num_numeric > 0:
            if num_categorical > 0:
                if structure['num_rows'] <= 10:  # Limit pie charts to 10 categories
                    viz_config['type'] = 'pie'
                    viz_config['names'] = structure['categorical_cols'][0]
                    viz_config['values'] = structure['numeric_cols'][0]
                else:
                    viz_config['type'] = 'bar'
                    viz_config['x'] = structure['categorical_cols'][0]
                    viz_config['y'] = structure['numeric_cols'][0]
            
        # Fallback to a simple visualization if no specific type was selected
        if not viz_config['type']:
            if num_numeric > 0 and num_categorical > 0:
                viz_config['type'] = 'bar'
                viz_config['x'] = structure['categorical_cols'][0]
                viz_config['y'] = structure['numeric_cols'][0]
            elif num_numeric >= 2:
                viz_config['type'] = 'scatter'
                viz_config['x'] = structure['numeric_cols'][0]
                viz_config['y'] = structure['numeric_cols'][1]
            elif num_numeric == 1:
                viz_config['type'] = 'histogram'
                viz_config['x'] = structure['numeric_cols'][0]
            else:
                viz_config['type'] = 'table'
                
            viz_config['confidence'] *= 0.5  # Lower confidence for fallback visualizations
            
        return viz_config

def render_visualization(viz_config: Dict[str, Any], df: pd.DataFrame, st) -> None:
    """Render the selected visualization in Streamlit."""
    if viz_config['type'] == 'value':
        st.metric(label="Result", value=viz_config['value'])
        return
        
    if viz_config['type'] == 'table':
        st.dataframe(df)
        return
        
    # Ensure we have proper column configuration for the visualization type
    structure = {
        'numeric_cols': df.select_dtypes(include=['int64', 'float64']).columns.tolist(),
        'categorical_cols': df.select_dtypes(include=['object', 'category']).columns.tolist(),
        'datetime_cols': df.select_dtypes(include=['datetime64']).columns.tolist()
    }
    
    # Configure columns based on visualization type
    if viz_config.get('x') is None or viz_config.get('y') is None:
        if viz_config['type'] in ['line', 'bar', 'scatter', 'box']:
            if structure['datetime_cols'] and viz_config['type'] == 'line':
                viz_config['x'] = structure['datetime_cols'][0]
            elif structure['categorical_cols']:
                viz_config['x'] = structure['categorical_cols'][0]
            elif structure['numeric_cols']:
                viz_config['x'] = structure['numeric_cols'][0]
                
            if structure['numeric_cols']:
                viz_config['y'] = structure['numeric_cols'][0]
                
        elif viz_config['type'] == 'histogram' and not viz_config.get('x'):
            if structure['numeric_cols']:
                viz_config['x'] = structure['numeric_cols'][0]
                
        elif viz_config['type'] == 'pie':
            if structure['categorical_cols'] and structure['numeric_cols']:
                viz_config['names'] = structure['categorical_cols'][0]
                viz_config['values'] = structure['numeric_cols'][0]
    
    # Create figure based on visualization type
    try:
        if viz_config['type'] == 'line':
            fig = px.line(df, 
                         x=viz_config['x'], 
                         y=viz_config['y'],
                         color=viz_config.get('color'))
                         
        elif viz_config['type'] == 'bar':
            fig = px.bar(df,
                        x=viz_config['x'],
                        y=viz_config['y'],
                        color=viz_config.get('color'))
                        
        elif viz_config['type'] == 'scatter':
            fig = px.scatter(df,
                            x=viz_config['x'],
                            y=viz_config['y'],
                            color=viz_config.get('color'))
                            
        elif viz_config['type'] == 'histogram':
            fig = px.histogram(df,
                             x=viz_config['x'],
                             color=viz_config.get('color'))
                             
        elif viz_config['type'] == 'box':
            fig = px.box(df,
                        x=viz_config['x'],
                        y=viz_config['y'],
                        color=viz_config.get('color'))
                        
        elif viz_config['type'] == 'pie':
            if not (viz_config.get('names') and viz_config.get('values')):
                st.error("Cannot create pie chart: requires both categorical and numeric columns")
                return
            fig = px.pie(df,
                        names=viz_config['names'],
                        values=viz_config['values'])
        else:
            st.error(f"Unsupported visualization type: {viz_config['type']}")
            return
            
        # Add confidence indicator if below threshold
        if viz_config['confidence'] < 0.7:
            st.warning(f"Note: This visualization was selected with {viz_config['confidence']:.0%} confidence. You may want to try a different type.")
            
        # Display the figure
        st.plotly_chart(fig)
        
    except Exception as e:
        st.error(f"Error creating visualization: {str(e)}")
        st.info("Try a different visualization type that better matches your data structure.")
