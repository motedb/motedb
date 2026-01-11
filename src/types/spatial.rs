//! Spatial geometry data type implementation

use serde::{Deserialize, Serialize};

/// 2D point
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    pub fn distance(&self, other: &Point) -> f64 {
        ((self.x - other.x).powi(2) + (self.y - other.y).powi(2)).sqrt()
    }
}

/// Bounding box for spatial indexing
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct BoundingBox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl BoundingBox {
    pub fn new(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        assert!(min_x <= max_x && min_y <= max_y, "Invalid bounding box");
        Self { min_x, min_y, max_x, max_y }
    }

    pub fn from_point(point: Point) -> Self {
        Self {
            min_x: point.x,
            min_y: point.y,
            max_x: point.x,
            max_y: point.y,
        }
    }

    pub fn contains(&self, point: &Point) -> bool {
        point.x >= self.min_x
            && point.x <= self.max_x
            && point.y >= self.min_y
            && point.y <= self.max_y
    }

    pub fn intersects(&self, other: &BoundingBox) -> bool {
        !(self.max_x < other.min_x
            || self.min_x > other.max_x
            || self.max_y < other.min_y
            || self.min_y > other.max_y)
    }

    pub fn expand(&mut self, point: &Point) {
        self.min_x = self.min_x.min(point.x);
        self.min_y = self.min_y.min(point.y);
        self.max_x = self.max_x.max(point.x);
        self.max_y = self.max_y.max(point.y);
    }

    pub fn area(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y)
    }
}

/// Geometry types supported by MoteDB
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Geometry {
    Point(Point),
    LineString(Vec<Point>),
    Polygon(Vec<Point>), // First point = last point for closed polygon
}

impl Geometry {
    /// Get bounding box of the geometry
    pub fn bounding_box(&self) -> BoundingBox {
        match self {
            Geometry::Point(p) => BoundingBox::from_point(*p),
            Geometry::LineString(points) | Geometry::Polygon(points) => {
                assert!(!points.is_empty(), "Empty geometry");
                let first = points[0];
                let mut bbox = BoundingBox::from_point(first);
                for point in &points[1..] {
                    bbox.expand(point);
                }
                bbox
            }
        }
    }

    /// Check if geometry intersects with a bounding box
    pub fn intersects_bbox(&self, bbox: &BoundingBox) -> bool {
        self.bounding_box().intersects(bbox)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_distance() {
        let p1 = Point::new(0.0, 0.0);
        let p2 = Point::new(3.0, 4.0);
        assert!((p1.distance(&p2) - 5.0).abs() < 0.001);
    }

    #[test]
    fn test_bbox_contains() {
        let bbox = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        assert!(bbox.contains(&Point::new(5.0, 5.0)));
        assert!(!bbox.contains(&Point::new(15.0, 5.0)));
    }

    #[test]
    fn test_bbox_intersects() {
        let bbox1 = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        let bbox2 = BoundingBox::new(5.0, 5.0, 15.0, 15.0);
        let bbox3 = BoundingBox::new(20.0, 20.0, 30.0, 30.0);
        
        assert!(bbox1.intersects(&bbox2));
        assert!(!bbox1.intersects(&bbox3));
    }

    #[test]
    fn test_geometry_bbox() {
        let polygon = Geometry::Polygon(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);
        
        let bbox = polygon.bounding_box();
        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.max_x, 10.0);
        assert_eq!(bbox.area(), 100.0);
    }
}
