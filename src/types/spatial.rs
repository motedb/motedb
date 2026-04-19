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

/// 3D point for embodied intelligence / point cloud use cases
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Point3D {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Point3D {
    pub fn new(x: f64, y: f64, z: f64) -> Self {
        Self { x, y, z }
    }

    pub fn distance_squared(&self, other: &Point3D) -> f64 {
        (self.x - other.x).powi(2)
            + (self.y - other.y).powi(2)
            + (self.z - other.z).powi(2)
    }

    pub fn distance(&self, other: &Point3D) -> f64 {
        self.distance_squared(other).sqrt()
    }

    pub fn to_f32(&self) -> [f32; 3] {
        [self.x as f32, self.y as f32, self.z as f32]
    }
}

/// 3D bounding box for i-Octree spatial queries
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct BoundingBox3D {
    pub min_x: f64,
    pub min_y: f64,
    pub min_z: f64,
    pub max_x: f64,
    pub max_y: f64,
    pub max_z: f64,
}

impl BoundingBox3D {
    pub fn new(min_x: f64, min_y: f64, min_z: f64, max_x: f64, max_y: f64, max_z: f64) -> Self {
        Self { min_x, min_y, min_z, max_x, max_y, max_z }
    }

    pub fn from_point(p: Point3D) -> Self {
        Self { min_x: p.x, min_y: p.y, min_z: p.z, max_x: p.x, max_y: p.y, max_z: p.z }
    }

    pub fn contains_point(&self, p: &Point3D) -> bool {
        p.x >= self.min_x && p.x <= self.max_x
            && p.y >= self.min_y && p.y <= self.max_y
            && p.z >= self.min_z && p.z <= self.max_z
    }

    pub fn intersects(&self, other: &BoundingBox3D) -> bool {
        !(self.max_x < other.min_x || self.min_x > other.max_x
            || self.max_y < other.min_y || self.min_y > other.max_y
            || self.max_z < other.min_z || self.min_z > other.max_z)
    }

    pub fn expand(&mut self, p: &Point3D) {
        self.min_x = self.min_x.min(p.x);
        self.min_y = self.min_y.min(p.y);
        self.min_z = self.min_z.min(p.z);
        self.max_x = self.max_x.max(p.x);
        self.max_y = self.max_y.max(p.y);
        self.max_z = self.max_z.max(p.z);
    }

    pub fn center(&self) -> Point3D {
        Point3D::new(
            (self.min_x + self.max_x) / 2.0,
            (self.min_y + self.max_y) / 2.0,
            (self.min_z + self.max_z) / 2.0,
        )
    }

    /// Half side-length (assumes cubic bounds)
    pub fn extent(&self) -> f64 {
        ((self.max_x - self.min_x).max(self.max_y - self.min_y).max(self.max_z - self.min_z)) / 2.0
    }

    pub fn volume(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y) * (self.max_z - self.min_z)
    }
}

/// Geometry types supported by MoteDB
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Geometry {
    Point(Point),
    Point3D(Point3D),
    LineString(Vec<Point>),
    Polygon(Vec<Point>), // First point = last point for closed polygon
}

impl Geometry {
    /// Get 2D bounding box of the geometry
    pub fn bounding_box(&self) -> BoundingBox {
        match self {
            Geometry::Point(p) => BoundingBox::from_point(*p),
            Geometry::Point3D(p) => BoundingBox::from_point(Point::new(p.x, p.y)),
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

    /// Get 3D bounding box (returns None for 2D-only geometries)
    pub fn bounding_box_3d(&self) -> Option<BoundingBox3D> {
        match self {
            Geometry::Point3D(p) => Some(BoundingBox3D::from_point(*p)),
            _ => None,
        }
    }

    /// Returns true if this geometry is 3D
    pub fn is_3d(&self) -> bool {
        matches!(self, Geometry::Point3D(_))
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

    #[test]
    fn test_point3d_distance() {
        let p1 = Point3D::new(1.0, 2.0, 3.0);
        let p2 = Point3D::new(4.0, 6.0, 3.0);
        assert!((p1.distance(&p2) - 5.0).abs() < 0.001);
    }

    #[test]
    fn test_bbox3d_contains() {
        let bbox = BoundingBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        assert!(bbox.contains_point(&Point3D::new(5.0, 5.0, 5.0)));
        assert!(!bbox.contains_point(&Point3D::new(15.0, 5.0, 5.0)));
        assert!(!bbox.contains_point(&Point3D::new(5.0, 5.0, 15.0)));
    }

    #[test]
    fn test_bbox3d_intersects() {
        let b1 = BoundingBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        let b2 = BoundingBox3D::new(5.0, 5.0, 5.0, 15.0, 15.0, 15.0);
        let b3 = BoundingBox3D::new(20.0, 20.0, 20.0, 30.0, 30.0, 30.0);
        assert!(b1.intersects(&b2));
        assert!(!b1.intersects(&b3));
    }

    #[test]
    fn test_geometry_point3d() {
        let g = Geometry::Point3D(Point3D::new(1.0, 2.0, 3.0));
        assert!(g.is_3d());
        assert!(g.bounding_box_3d().is_some());
        let bbox = g.bounding_box_3d().unwrap();
        assert_eq!(bbox.center(), Point3D::new(1.0, 2.0, 3.0));
    }
}
