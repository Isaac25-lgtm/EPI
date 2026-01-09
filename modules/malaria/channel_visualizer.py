"""
Endemic Channel Visualizer
Creates interactive visualizations for malaria surveillance
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd

from modules.malaria.config import COLORS, ALERT_ZONES, CHART_CONFIG
from modules.malaria.utils import format_week_label, format_week_date


class EndemicChannelVisualizer:
    """Create visualizations for endemic channel analysis"""
    
    def __init__(self, orgunit_name, current_year):
        self.orgunit_name = orgunit_name
        self.current_year = current_year
    
    def create_channel_chart(self, channel_df, current_df, analysis_df):
        """
        Create main endemic channel chart with current year overlay
        Args:
            channel_df: Channel thresholds
            current_df: Current year data
            analysis_df: Analysis results with alerts
        Returns:
            Plotly figure object
        """
        fig = go.Figure()
        
        # Channel fill (between Q1 and Q3)
        fig.add_trace(go.Scatter(
            x=channel_df['epi_week'],
            y=channel_df['q3'],
            mode='lines',
            name='75th Percentile (Epidemic Threshold)',
            line=dict(color=COLORS['q3_line'], width=2, dash='dash'),
            hovertemplate='<b>Week %{x}</b><br>75th Percentile: %{y:.0f} cases<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=channel_df['epi_week'],
            y=channel_df['q1'],
            mode='lines',
            name='25th Percentile',
            line=dict(color=COLORS['q1_line'], width=1, dash='dot'),
            fill='tonexty',
            fillcolor=COLORS['channel_fill'],
            hovertemplate='<b>Week %{x}</b><br>25th Percentile: %{y:.0f} cases<extra></extra>'
        ))
        
        # Median line
        fig.add_trace(go.Scatter(
            x=channel_df['epi_week'],
            y=channel_df['median'],
            mode='lines',
            name='Median (Expected)',
            line=dict(color=COLORS['median_line'], width=2),
            hovertemplate='<b>Week %{x}</b><br>Median: %{y:.0f} cases<extra></extra>'
        ))
        
        # Current year line
        fig.add_trace(go.Scatter(
            x=current_df['epi_week'],
            y=current_df['confirmed_cases'],
            mode='lines+markers',
            name=f'{self.current_year} Cases',
            line=dict(color=COLORS['current_year'], width=3),
            marker=dict(size=6, color=COLORS['current_year']),
            hovertemplate='<b>Week %{x}</b><br>Cases: %{y:.0f}<extra></extra>'
        ))
        
        # Alert markers (red dots on weeks exceeding threshold)
        alert_weeks = analysis_df[analysis_df['is_alert']]
        if len(alert_weeks) > 0:
            fig.add_trace(go.Scatter(
                x=alert_weeks['epi_week'],
                y=alert_weeks['confirmed_cases'],
                mode='markers',
                name='Alert Weeks',
                marker=dict(
                    size=12,
                    color=COLORS['alert_marker'],
                    symbol='circle-open',
                    line=dict(width=3, color=COLORS['alert_marker'])
                ),
                hovertemplate='<b>⚠️ ALERT - Week %{x}</b><br>Cases: %{y:.0f}<extra></extra>'
            ))
        
        # Layout
        fig.update_layout(
            title=dict(
                text=f'Malaria Endemic Channel - {self.orgunit_name}<br><sub>Year {self.current_year}</sub>',
                font=dict(size=CHART_CONFIG['title_font_size'], color='#2c3e50')
            ),
            xaxis=dict(
                title='Epidemiological Week',
                tickmode='linear',
                tick0=1,
                dtick=4,
                gridcolor=COLORS['grid'],
                showgrid=CHART_CONFIG['show_grid']
            ),
            yaxis=dict(
                title='Confirmed Malaria Cases',
                gridcolor=COLORS['grid'],
                showgrid=CHART_CONFIG['show_grid']
            ),
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            hovermode='x unified',
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=-0.2,
                xanchor='center',
                x=0.5,
                font=dict(size=CHART_CONFIG['legend_font_size'])
            ),
            height=CHART_CONFIG['height'],
            width=CHART_CONFIG['width']
        )
        
        return fig
    
    def create_zone_distribution_chart(self, zone_distribution):
        """
        Create pie chart showing distribution across alert zones
        Args:
            zone_distribution: Dictionary from calculator.get_zone_distribution()
        Returns:
            Plotly figure
        """
        zones = ['epidemic', 'alert', 'safety', 'success']
        labels = [ALERT_ZONES[z]['name'] for z in zones]
        values = [zone_distribution[z]['count'] for z in zones]
        colors = [ALERT_ZONES[z]['color'] for z in zones]
        
        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            marker=dict(colors=colors),
            hole=0.4,
            textinfo='label+percent',
            hovertemplate='<b>%{label}</b><br>Weeks: %{value}<br>Percentage: %{percent}<extra></extra>'
        )])
        
        fig.update_layout(
            title='Alert Zone Distribution',
            annotations=[dict(text=f'{sum(values)}<br>Weeks', x=0.5, y=0.5, font_size=16, showarrow=False)],
            height=400,
            showlegend=True
        )
        
        return fig
    
    def create_comparison_chart(self, year_comparisons):
        """
        Create bar chart comparing current year to baseline years
        Args:
            year_comparisons: Dictionary from calculator.compare_years()
        Returns:
            Plotly figure
        """
        # Prepare data
        years = []
        total_cases = []
        colors_list = []
        
        for year, data in year_comparisons.items():
            if year == 'current':
                years.append(f'{self.current_year} (Current)')
                colors_list.append('#e74c3c')
            elif year == 'baseline_avg':
                years.append('Baseline Average')
                colors_list.append('#3498db')
            else:
                years.append(str(year))
                colors_list.append('#95a5a6')
            
            total_cases.append(data['total_cases'])
        
        fig = go.Figure(data=[
            go.Bar(
                x=years,
                y=total_cases,
                marker_color=colors_list,
                text=total_cases,
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Total Cases: %{y:,}<extra></extra>'
            )
        ])
        
        fig.update_layout(
            title=f'Total Cases Comparison: {self.current_year} vs Baseline Years',
            xaxis_title='Year',
            yaxis_title='Total Confirmed Cases',
            height=450,
            showlegend=False
        )
        
        return fig
    
    def create_trend_chart(self, analysis_df):
        """
        Create line chart showing trend with moving average
        Args:
            analysis_df: Analysis DataFrame
        Returns:
            Plotly figure
        """
        # Calculate 4-week moving average
        analysis_df['ma_4week'] = analysis_df['confirmed_cases'].rolling(window=4).mean()
        
        fig = go.Figure()
        
        # Actual cases
        fig.add_trace(go.Scatter(
            x=analysis_df['epi_week'],
            y=analysis_df['confirmed_cases'],
            mode='lines+markers',
            name='Actual Cases',
            line=dict(color='#34495e', width=2),
            marker=dict(size=5),
            opacity=0.6
        ))
        
        # Moving average
        fig.add_trace(go.Scatter(
            x=analysis_df['epi_week'],
            y=analysis_df['ma_4week'],
            mode='lines',
            name='4-Week Moving Average',
            line=dict(color='#e74c3c', width=3)
        ))
        
        # Median baseline
        fig.add_trace(go.Scatter(
            x=analysis_df['epi_week'],
            y=analysis_df['median'],
            mode='lines',
            name='Baseline Median',
            line=dict(color='#3498db', width=2, dash='dash')
        ))
        
        fig.update_layout(
            title=f'Malaria Cases Trend - {self.current_year}',
            xaxis_title='Epidemiological Week',
            yaxis_title='Confirmed Cases',
            hovermode='x unified',
            height=400
        )
        
        return fig
    
    def create_alert_table_html(self, analysis_df):
        """
        Create HTML table of alert weeks
        Args:
            analysis_df: Analysis DataFrame
        Returns:
            HTML string
        """
        alert_df = analysis_df[analysis_df['is_alert']].copy()
        
        if len(alert_df) == 0:
            return '<div class="alert alert-success">✅ No epidemic alerts detected</div>'
        
        alert_df = alert_df.sort_values('epi_week')
        
        html = '''
        <table class="alert-table">
            <thead>
                <tr>
                    <th>Week</th>
                    <th>Cases</th>
                    <th>Threshold (75th)</th>
                    <th>Deviation</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for _, row in alert_df.iterrows():
            deviation = row['deviation_percent']
            deviation_str = f"+{deviation:.1f}%" if deviation > 0 else f"{deviation:.1f}%"
            
            html += f'''
                <tr>
                    <td><strong>Week {row['epi_week']:02d}</strong></td>
                    <td>{row['confirmed_cases']:.0f}</td>
                    <td>{row['q3']:.0f}</td>
                    <td class="deviation-cell">{deviation_str}</td>
                    <td><span class="status-badge status-alert">{row['alert_status']}</span></td>
                </tr>
            '''
        
        html += '''
            </tbody>
        </table>
        '''
        
        return html
    
    def create_dashboard_summary_html(self, summary_stats, alert_summary, zone_distribution):
        """
        Create HTML summary cards for dashboard
        Args:
            summary_stats: From utils.calculate_summary_stats()
            alert_summary: From calculator.get_alert_summary()
            zone_distribution: From calculator.get_zone_distribution()
        Returns:
            HTML string
        """
        html = f'''
        <div class="summary-cards">
            <div class="summary-card card-current">
                <div class="card-icon">📅</div>
                <div class="card-content">
                    <div class="card-value">{summary_stats['current_week']}</div>
                    <div class="card-label">Current Week</div>
                    <div class="card-sublabel">{summary_stats['current_week_cases']} cases</div>
                </div>
            </div>
            
            <div class="summary-card card-alerts">
                <div class="card-icon">⚠️</div>
                <div class="card-content">
                    <div class="card-value">{summary_stats['total_alert_weeks']}</div>
                    <div class="card-label">Alert Weeks YTD</div>
                    <div class="card-sublabel">{summary_stats['alert_rate']}% of weeks</div>
                </div>
            </div>
            
            <div class="summary-card card-total">
                <div class="card-icon">📊</div>
                <div class="card-content">
                    <div class="card-value">{summary_stats['total_cases_ytd']:,}</div>
                    <div class="card-label">Total Cases YTD</div>
                    <div class="card-sublabel">vs {summary_stats['expected_cases_ytd']:,} expected</div>
                </div>
            </div>
            
            <div class="summary-card card-deviation">
                <div class="card-icon">{'📈' if summary_stats['deviation_percent'] > 0 else '📉'}</div>
                <div class="card-content">
                    <div class="card-value">{summary_stats['deviation_percent']:+.1f}%</div>
                    <div class="card-label">vs Baseline</div>
                    <div class="card-sublabel">{summary_stats['current_week_status']}</div>
                </div>
            </div>
        </div>
        '''
        
        return html
