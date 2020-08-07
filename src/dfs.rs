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

/// Run depth-first-search over a KripkeStructure.
pub fn dfs<KS>(ks: KS, max_depth: Option<usize>) -> Result<(), DfsError>
where
    KS: KripkeStructure + Copy,
    <KS as KripkeStructure>::Label: std::fmt::Debug,
{
    let mut stack: PathState<KS::Label, KS::LabelIterator> = PathState::new();

    ks.restart();
    stack.append(ks.successors());

    while !stack.is_empty() {
        if let Some(d) = max_depth {
            if stack.items.len() >= d {
                return Err(DfsError::MaxDepthExceeded(d));
            }
        }

        ks.restart();
        for (_, label) in stack.items.iter().copied().enumerate() {
            ks.transition(label);
        }
        if !stack.append(ks.successors()) {
            stack.advance();
        }
    }
    Ok(())
}
