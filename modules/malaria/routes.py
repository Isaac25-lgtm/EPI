"""
Flask routes for Malaria Endemic Channel module
"""

from flask import render_template, request, jsonify
from datetime import datetime
import traceback

from modules.malaria import malaria_bp
from modules.malaria.data_processor import MalariaDataProcessor
from modules.malaria.channel_calculator import EndemicChannelCalculator
from modules.malaria.channel_visualizer import EndemicChannelVisualizer
from modules.malaria.utils import calculate_summary_stats


@malaria_bp.route('/')
def malaria_dashboard():
    """Render malaria endemic channel dashboard"""
    return render_template('malaria.html')


@malaria_bp.route('/api/channel-data')
def get_channel_data():
    """
    API endpoint to get endemic channel data
    Query params:
        - orgunit: Organization unit ID
        - year: Current year for monitoring (optional, defaults to current year)
        - threshold: Threshold percentile ('q3' or 'q85', default 'q3')
        - consecutive_weeks: Number of consecutive weeks for alert (default 1)
    """
    try:
        # Get parameters
        orgunit_id = request.args.get('orgunit')
        current_year = int(request.args.get('year', datetime.now().year))
        threshold = request.args.get('threshold', 'q3')
        consecutive_weeks = int(request.args.get('consecutive_weeks', 1))
        apply_consecutive_rule = consecutive_weeks > 1
        
        if not orgunit_id:
            return jsonify({'error': 'Organization unit ID required'}), 400
        
        # Initialize components
        processor = MalariaDataProcessor()
        calculator = EndemicChannelCalculator(
            threshold_percentile=threshold,
            apply_consecutive_rule=apply_consecutive_rule,
            consecutive_weeks=consecutive_weeks
        )
        
        # Fetch and process data
        baseline_df = processor.prepare_baseline_data(orgunit_id, current_year)
        current_df = processor.prepare_current_data(orgunit_id, current_year)
        
        if baseline_df.empty:
            return jsonify({'error': 'No baseline data available'}), 404
        
        if current_df.empty:
            return jsonify({'error': 'No current year data available'}), 404
        
        # Calculate endemic channel
        channel_df = calculator.calculate_channel(baseline_df)
        
        # Detect alerts
        analysis_df = calculator.detect_alerts(current_df, channel_df)
        
        # Calculate z-scores
        analysis_df = calculator.calculate_z_scores(analysis_df)
        
        # Get summaries
        alert_summary = calculator.get_alert_summary(analysis_df)
        zone_distribution = calculator.get_zone_distribution(analysis_df)
        year_comparisons = calculator.compare_years(baseline_df, current_df, channel_df)
        trend = calculator.get_trend_indicator(analysis_df)
        
        # Prepare response
        response_data = {
            'channel': channel_df.to_dict('records'),
            'current_data': current_df[['epi_week', 'confirmed_cases']].to_dict('records'),
            'analysis': analysis_df[[
                'epi_week', 'confirmed_cases', 'q1', 'median', 'q3', 'q85',
                'is_alert', 'is_confirmed_alert', 'alert_zone', 'alert_status',
                'deviation_percent', 'z_score'
            ]].to_dict('records'),
            'alert_summary': alert_summary,
            'zone_distribution': zone_distribution,
            'year_comparisons': year_comparisons,
            'trend': trend,
            'metadata': {
                'orgunit_id': orgunit_id,
                'current_year': current_year,
                'threshold': threshold,
                'baseline_years': sorted(baseline_df['year'].unique().tolist()),
                'data_element': 'tE0G64cijAT'
            }
        }
        
        return jsonify(response_data)
    
    except Exception as e:
        print(f"Error in get_channel_data: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/export-data')
def export_channel_data():
    """
    Export channel data as CSV
    Same query params as /api/channel-data
    """
    try:
        # Get parameters
        orgunit_id = request.args.get('orgunit')
        current_year = int(request.args.get('year', datetime.now().year))
        threshold = request.args.get('threshold', 'q3')
        
        if not orgunit_id:
            return jsonify({'error': 'Organization unit ID required'}), 400
        
        # Initialize components
        processor = MalariaDataProcessor()
        calculator = EndemicChannelCalculator(threshold_percentile=threshold)
        
        # Fetch and process data
        baseline_df = processor.prepare_baseline_data(orgunit_id, current_year)
        current_df = processor.prepare_current_data(orgunit_id, current_year)
        channel_df = calculator.calculate_channel(baseline_df)
        analysis_df = calculator.detect_alerts(current_df, channel_df)
        
        # Merge for export
        export_df = analysis_df[[
            'epi_week', 'confirmed_cases', 'q1', 'median', 'q3', 'q85',
            'is_alert', 'alert_zone', 'alert_status', 'deviation_percent'
        ]].copy()
        
        export_df.columns = [
            'Epi Week', 'Cases', 'Q1 (25th)', 'Median (50th)', 'Q3 (75th)', 'Q85 (85th)',
            'Is Alert', 'Alert Zone', 'Status', 'Deviation %'
        ]
        
        # Convert to CSV
        csv_data = export_df.to_csv(index=False)
        
        from flask import Response
        return Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment;filename=malaria_channel_{orgunit_id}_{current_year}.csv'}
        )
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@malaria_bp.route('/api/orgunit-search')
def search_orgunits():
    """
    Search for organization units
    Query params:
        - query: Search term
        - level: Org unit level (optional)
    """
    try:
        query = request.args.get('query', '')
        level = request.args.get('level')
        
        processor = MalariaDataProcessor()
        
        # Build DHIS2 API URL for org unit search
        import requests
        url = f"{processor.dhis2_url}/api/organisationUnits.json"
        
        params = {
            'fields': 'id,name,level',
            'filter': f'name:ilike:{query}',
            'paging': 'false'
        }
        
        if level:
            params['filter'] += f'&level:eq:{level}'
        
        response = requests.get(
            url,
            params=params,
            auth=(processor.dhis2_user, processor.dhis2_pass),
            timeout=30
        )
        
        if response.status_code != 200:
            return jsonify({'error': 'Failed to search organization units'}), 500
        
        data = response.json()
        orgunits = data.get('organisationUnits', [])
        
        return jsonify({'orgunits': orgunits})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
