use crate::KripkeStructure;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum DfsError {
    #[error("depth exceeded max depth {0}")]
    MaxDepthExceeded(usize),
}

/// Store the stack for DFS state.
///
/// This separates the label iterators from the current labels, to try to get better cache locality
/// when restarting trajectories.
#[derive(Clone, Debug)]
struct PathState<T, I> {
    iters: Vec<I>,
    items: Vec<T>,
}
impl<T, I> PathState<T, I> {
    fn new() -> Self {
        let iters = Vec::new();
        let items = Vec::new();
        Self { iters, items }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.iters.is_empty()
    }

    #[inline]
    fn append(&mut self, maybe_iter: Option<I>) -> bool
    where
        I: Iterator<Item = T>,
    {
        match maybe_iter {
            None => false,
            Some(mut iter) => match iter.next() {
                None => false,
                Some(item) => {
                    self.iters.push(iter);
                    self.items.push(item);
                    true
                }
            },
        }
    }

    #[inline]
    fn advance(&mut self) -> bool
    where
        I: Iterator<Item = T>,
    {
        //let _l = self.items.len();
        while !self.items.is_empty() {
            let last_iter = self.iters.last_mut().unwrap();
            //let last_iter = unsafe { self.iters.get_unchecked_mut(l - 1) };
            match last_iter.next() {
                Some(item) => {
                    //let last_item = unsafe { self.items.get_unchecked_mut(l - 1) };
                    //*last_item = item;
                    *self.items.last_mut().unwrap() = item;
                    return true;
                }
                None => {
                    //unsafe { self.iters.set_len(l - 1) };
                    //unsafe { self.items.set_len(l - 1) };
                    //l -= 1;
                    self.iters.pop();
                    self.items.pop();
                }
            }
        }
        false
    }
}

/// An iterator to execute depth-first-search over a KripkeStructure.
pub struct Dfs<KS, L, I> {
    ks: KS,
    max_depth: Option<usize>,

    stack: PathState<L, I>,
}

impl<KS> Dfs<KS, KS::Label, KS::LabelIterator>
where
    KS: KripkeStructure + Copy,
    Self: Iterator<Item = Result<(), DfsError>>,
{
    pub fn new(ks: KS, max_depth: Option<usize>) -> Self {
        let mut stack = PathState::new();
        ks.restart();
        stack.append(ks.successors());
        Self {
            ks,
            max_depth,
            stack,
        }
    }

    pub fn run_to_completion(&mut self) -> Result<(), DfsError> {
        self.collect()
    }

    #[inline]
    pub fn restart(&mut self) {
        self.stack.items.clear();
        self.stack.iters.clear();
        self.ks.restart();
        self.stack.append(self.ks.successors());
    }
}

impl<KS> Iterator for Dfs<KS, KS::Label, KS::LabelIterator>
where
    KS: KripkeStructure + Copy,
{
    type Item = Result<(), DfsError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // The negative case is probably more common, so make it the first branch.
        if !self.stack.is_empty() {
            self.ks.restart();
            for (_, label) in self.stack.items.iter().copied().enumerate() {
                self.ks.transition(label);
            }

            // The negative case is probably more common, so make it the first branch.
            if !self.stack.append(self.ks.successors()) {
                self.stack.advance();
            } else if let Some(d) = self.max_depth {
                let l = self.stack.items.len();
                if l > d {
                    return Some(Err(DfsError::MaxDepthExceeded(l)));
                }
            }
            Some(Ok(()))
        } else {
            None
        }
    }
}

/// Run depth-first-search over a KripkeStructure.
pub fn dfs<KS>(ks: KS, max_depth: Option<usize>) -> Result<(), DfsError>
where
    KS: KripkeStructure + Copy,
{
    Dfs::new(ks, max_depth).run_to_completion()
}
