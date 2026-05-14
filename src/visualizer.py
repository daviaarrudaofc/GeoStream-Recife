"""Visualization module for geospatial data."""
from pathlib import Path
from typing import Optional
import plotly.express as px
import plotly.graph_objects as go
import geopandas as gpd
from src.logger import setup_logger
from src.config import GEO

logger = setup_logger(__name__)


class Visualizer:
    """Handles all visualization tasks."""
    
    OUTPUT_DIR = Path(__file__).parent.parent / "outputs"
    
    def __init__(self):
        """Initialize visualizer."""
        self.OUTPUT_DIR.mkdir(exist_ok=True)
        logger.info(f"Visualizer initialized. Output directory: {self.OUTPUT_DIR}")
    
    def create_choropleth_map(
        self,
        gdf: gpd.GeoDataFrame,
        color_column: str = "densidade",
        title: str = "Densidade de Equipamentos Públicos - GeoStream Recife",
        output_file: Optional[Path] = None
    ) -> go.Figure:
        """
        Create choropleth map visualization.
        
        Args:
            gdf: GeoDataFrame with geometry
            color_column: Column to color by
            title: Map title
            output_file: Optional output file path
            
        Returns:
            Plotly figure
        """
        logger.info(f"Creating choropleth map with {len(gdf)} features")
        
        try:
            fig = px.choropleth_mapbox(
                gdf,
                geojson=gdf.__geo_interface__,
                locations=gdf.index,
                color=color_column,
                color_continuous_scale=GEO.DEFAULT_COLORSCALE,
                mapbox_style="carto-positron",
                zoom=GEO.DEFAULT_ZOOM,
                center={"lat": GEO.RECIFE_LAT, "lon": GEO.RECIFE_LON},
                opacity=0.75,
                title=title,
                labels={color_column: 'Equipamentos por Hexágono'}
            )
            
            fig.update_traces(marker_line_width=0.5, marker_line_color="gray")
            fig.update_layout(
                margin={"r": 10, "t": 90, "l": 10, "b": 30},
                height=760,
                hovermode="closest",
                template="plotly_white",
                title={
                    "text": title,
                    "x": 0.5,
                    "xanchor": "center",
                    "font": {"size": 24}
                },
                coloraxis_colorbar={
                    "title": "Equipamentos por Hexágono",
                    "ticks": "outside",
                    "thickness": 20,
                    "lenmode": "fraction",
                    "len": 0.4
                },
                legend=dict(bgcolor="rgba(255,255,255,0.92)", bordercolor="#CCCCCC", borderwidth=1),
                paper_bgcolor="#F7F7F7",
                plot_bgcolor="#F7F7F7"
            )
            fig.update_layout(
                annotations=[
                    {
                        "text": "Fonte: dados públicos de Recife | Hexágonos H3 com resolução 8",
                        "showarrow": False,
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0,
                        "y": -0.05,
                        "xanchor": "left",
                        "font": {"size": 11, "color": "#555555"}
                    }
                ]
            )
            
            if output_file:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                fig.write_html(output_file)
                logger.info(f"Map saved to {output_file}")
            
            return fig
            
        except Exception as e:
            logger.error(f"Error creating choropleth map: {e}")
            raise
    
    def create_scatter_map(
        self,
        df,
        title: str = "Equipamentos Públicos - GeoStream Recife",
        output_file: Optional[Path] = None
    ) -> go.Figure:
        """Create scatter map of points."""
        logger.info(f"Creating scatter map with {len(df)} points")
        
        try:
            fig = px.scatter_mapbox(
                df,
                lat="lat",
                lon="lon",
                hover_name="nome",
                title=title,
                zoom=GEO.DEFAULT_ZOOM,
                center={"lat": GEO.RECIFE_LAT, "lon": GEO.RECIFE_LON},
                mapbox_style="carto-positron"
            )
            
            fig.update_layout(height=700)
            
            if output_file:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                fig.write_html(output_file)
                logger.info(f"Scatter map saved to {output_file}")
            
            return fig
            
        except Exception as e:
            logger.error(f"Error creating scatter map: {e}")
            raise
    
    def create_cluster_map(
        self,
        df,
        centers=None,
        title: str = "K-means Clustering - GeoStream Recife",
        output_file: Optional[Path] = None
    ) -> go.Figure:
        """Create map with cluster visualization."""
        logger.info("Creating cluster map")
        
        try:
            fig = px.scatter_mapbox(
                df,
                lat="lat",
                lon="lon",
                hover_name="nome",
                color=df["cluster"].astype(str),
                title=title,
                zoom=GEO.DEFAULT_ZOOM,
                center={"lat": GEO.RECIFE_LAT, "lon": GEO.RECIFE_LON},
                mapbox_style="open-street-map",
                color_discrete_sequence=px.colors.qualitative.Safe,
                labels={"color": "Cluster"}
            )
            
            marker_symbols = ["circle", "square", "diamond", "cross", "x"]
            for index, trace in enumerate(fig.data):
                if trace.name and trace.name != "Centroides de Cluster":
                    symbol = marker_symbols[index % len(marker_symbols)]
                    trace.marker.update(symbol=symbol)
            
            fig.update_traces(marker=dict(size=14, opacity=0.92))
            fig.update_layout(
                margin={"r": 10, "t": 90, "l": 10, "b": 30},
                height=760,
                template="plotly_white",
                title={
                    "text": title,
                    "x": 0.5,
                    "xanchor": "center",
                    "font": {"size": 24}
                },
                legend_title_text="Clusters",
                legend=dict(bgcolor="rgba(255,255,255,0.92)", bordercolor="#CCCCCC", borderwidth=1, orientation="h", y=1.02, x=0.5, xanchor="center"),
                paper_bgcolor="#F7F7F7",
                plot_bgcolor="#F7F7F7",
                mapbox=dict(style="open-street-map", center={"lat": GEO.RECIFE_LAT, "lon": GEO.RECIFE_LON}, zoom=GEO.DEFAULT_ZOOM)
            )
            
            if centers is not None and len(centers) > 0:
                fig.add_trace(
                    go.Scattermapbox(
                        lat=centers[:, 0],
                        lon=centers[:, 1],
                        mode="markers",
                        marker=dict(size=20, symbol="star", color="#222222", opacity=1.0),
                        name="Centroides de Cluster",
                        hoverinfo="text",
                        hovertext=[f"Centro do Cluster {i}" for i in range(len(centers))]
                    )
                )
                fig.update_layout(legend=dict(itemsizing="constant"))
            
            fig.update_layout(
                annotations=[
                    {
                        "text": "Fonte: dados públicos de Recife | Agrupamento K-means de equipamentos",
                        "showarrow": False,
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0,
                        "y": -0.05,
                        "xanchor": "left",
                        "font": {"size": 11, "color": "#555555"}
                    }
                ]
            )
            
            if output_file:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                fig.write_html(output_file)
                logger.info(f"Cluster map saved to {output_file}")
            
            return fig
            
        except Exception as e:
            logger.error(f"Error creating cluster map: {e}")
            raise
    
    def show(self, fig: go.Figure):
        """Display figure in browser."""
        logger.info("Opening map in browser")
        fig.show()
